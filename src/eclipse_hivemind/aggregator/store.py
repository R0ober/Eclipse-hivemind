"""Small in-memory store used by the first aggregator implementation."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from pathlib import Path

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
            experiment.events[event.event_id] = (known_node, event)
            accepted += 1
            self._append_export(experiment_id, known_node, event)
            if last_sequence is None or event.sequence > last_sequence:
                last_sequence = event.sequence

        return accepted, duplicates, last_sequence

    def summary(self, experiment_id: str) -> tuple[int, int]:
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
            "experiment_id": experiment_id,
            "node": node.model_dump(mode="json"),
            "event": event.model_dump(mode="json"),
        }
        with self._export_path.open("a", encoding="utf-8") as export_file:
            export_file.write(json.dumps(record, sort_keys=True) + "\n")
