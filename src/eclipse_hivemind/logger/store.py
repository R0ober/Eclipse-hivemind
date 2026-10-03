"""Small in-memory store used by the first logger implementation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock

from .models import Event, ExperimentRegistration, NodeIdentity


class ExperimentAlreadyExists(Exception):
    """Raised when an experiment is registered twice."""


class UnknownExperiment(Exception):
    """Raised when an event references an unregistered experiment."""


class UnknownNode(Exception):
    """Raised when an event references a node outside the manifest."""


class EventConflict(Exception):
    """Raised when an event ID is reused with different data."""


@dataclass
class StoredExperiment:
    registration: ExperimentRegistration
    events: dict[str, tuple[NodeIdentity, Event]] = field(default_factory=dict)


class InMemoryEventStore:
    """Store experiment manifests and events until persistent storage exists."""

    def __init__(self, export_path: Path | None = None) -> None:
        self._experiments: dict[str, StoredExperiment] = {}
        self._export_path = export_path
        # HTTP handlers run in worker threads; acceptance and export are one operation.
        self._lock = RLock()

    def register(self, registration: ExperimentRegistration) -> None:
        if registration.experiment_id in self._experiments:
            raise ExperimentAlreadyExists(registration.experiment_id)
        self._experiments[registration.experiment_id] = StoredExperiment(registration)

    def submit(
        self,
        experiment_id: str,
        node_id: str,
        node_type: str,
        node_index: int,
        events: list[Event],
    ) -> tuple[int, int, int | None]:
        """Persist new events and recognise retries with identical contents.

        Hold the lock across export and acceptance so concurrent submissions
        cannot write the same event twice. A failed export leaves that event
        unaccepted; earlier events in the batch may already have been saved.
        """
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if experiment is None:
                raise UnknownExperiment(experiment_id)

            known_nodes = {
                node.node_id: node for node in experiment.registration.nodes
            }
            known_node = known_nodes.get(node_id)
            if (
                known_node is None
                or known_node.node_type != node_type
                or known_node.node_index != node_index
            ):
                raise UnknownNode(node_id)

            accepted = 0
            duplicates = 0
            last_sequence: int | None = None
            for event in events:
                existing = experiment.events.get(event.event_id)
                if existing is not None:
                    if existing != (known_node, event):
                        raise EventConflict(event.event_id)
                    duplicates += 1
                    continue
                # Persist before remembering acceptance: a failed write must remain
                # retryable rather than becoming an acknowledged duplicate.
                self._append_export(experiment_id, known_node, event)
                experiment.events[event.event_id] = (known_node, event)
                accepted += 1
                if last_sequence is None or event.sequence > last_sequence:
                    last_sequence = event.sequence

            return accepted, duplicates, last_sequence

    def start_barrier(
        self,
        experiment_id: str,
        node_id: str,
    ) -> tuple[str, list[str], list[str]]:
        """Return the start group of one node, and which nodes in it have started.

        The barrier is per group, not per experiment: a node started by a later
        startup phase must be able to join a swarm that is already training.
        """
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if experiment is None:
                raise UnknownExperiment(experiment_id)

            nodes = {node.node_id: node for node in experiment.registration.nodes}
            asking_node = nodes.get(node_id)
            if asking_node is None:
                raise UnknownNode(node_id)

            started = {
                node.node_id
                for node, event in experiment.events.values()
                if event.event_type == "node_started"
            }
            group = [
                node.node_id
                for node in experiment.registration.nodes
                if node.start_group == asking_node.start_group
            ]
            return (
                asking_node.start_group,
                [member for member in group if member in started],
                [member for member in group if member not in started],
            )

    def summary(self, experiment_id: str) -> tuple[int, int]:
        """Count accepted events and reporting nodes without interpreting measurements."""
        with self._lock:
            experiment = self._experiments.get(experiment_id)
            if experiment is None:
                raise UnknownExperiment(experiment_id)
            node_ids = {node.node_id for node, _event in experiment.events.values()}
            return len(experiment.events), len(node_ids)

    def _append_export(
        self,
        experiment_id: str,
        node: NodeIdentity,
        event: Event,
    ) -> None:
        if self._export_path is None:
            return

        self._export_path.parent.mkdir(parents=True, exist_ok=True)
        record = {
            "event": {
                "event_type": event.event_type,
                "event_id": event.event_id,
                "sequence": event.sequence,
                "occurred_at": event.occurred_at.isoformat(),
                "data": event.data,
            },
            "node": {
                "node_id": node.node_id,
                "node_type": node.node_type,
                "node_index": node.node_index,
            },
            "experiment_id": experiment_id,
        }
        with self._export_path.open("a", encoding="utf-8") as export_file:
            export_file.write(json.dumps(record) + "\n")
