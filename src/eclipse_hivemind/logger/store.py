"""Disk-backed event history with incrementally maintained runtime state."""

from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path

from .models import Event, EventType, ExperimentNode, ExperimentRegistration
from .eclipse_metrics import holder_metrics


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
    nodes: dict[str, ExperimentNode]


class SQLiteEventStore:
    """Keep history and deduplication on disk; status reads use current node rows.

    Docker uses events.sqlite3 beside events.jsonl. Without an output path, use
    a temporary disk database, including for injected test stores. One lock
    serializes the connection and JSONL writes across FastAPI threads. The
    service uses one process, as configured by the supplied Uvicorn entry point.
    """

    def __init__(self, export_path: Path | None = None, *, database_path: Path | None = None) -> None:
        self._lock = threading.RLock()
        self._temporary_directory = None
        self._closed = False
        self._export_path = Path(export_path).resolve() if export_path is not None else None
        if database_path is None:
            if self._export_path is not None:
                database_path = self._export_path.with_suffix(".sqlite3")
            else:
                self._temporary_directory = tempfile.TemporaryDirectory(prefix="eclipse-event-store-")
                database_path = Path(self._temporary_directory.name) / "events.sqlite3"
        self.database_path = Path(database_path).resolve()
        self.database_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = sqlite3.connect(self.database_path, check_same_thread=False)
        self._db.row_factory = sqlite3.Row
        try:
            # Bound the page-cache target and avoid history-sized memory mappings.
            # Rollback journals work on Docker Desktop's mounted output folder.
            self._db.execute("PRAGMA journal_mode = DELETE")
            self._db.execute("PRAGMA synchronous = FULL")
            self._db.execute("PRAGMA cache_size = -4096")
            self._db.execute("PRAGMA mmap_size = 0")
            self._db.execute("PRAGMA temp_store = FILE")
            self._db.execute("PRAGMA foreign_keys = ON")
            self._db.executescript("""
                CREATE TABLE IF NOT EXISTS experiments (
                    experiment_id TEXT PRIMARY KEY,
                    registration_json TEXT NOT NULL,
                    event_count INTEGER NOT NULL DEFAULT 0
                );
                CREATE TABLE IF NOT EXISTS events (
                    ordinal INTEGER PRIMARY KEY,
                    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
                    event_id TEXT NOT NULL,
                    node_id TEXT NOT NULL,
                    record_json TEXT NOT NULL,
                    UNIQUE (experiment_id, event_id)
                );
                CREATE TABLE IF NOT EXISTS node_state (
                    experiment_id TEXT NOT NULL REFERENCES experiments(experiment_id),
                    node_id TEXT NOT NULL,
                    has_events INTEGER NOT NULL DEFAULT 0,
                    started INTEGER NOT NULL DEFAULT 0,
                    finished INTEGER NOT NULL DEFAULT 0,
                    latest_eval_round INTEGER,
                    runtime_metadata_json TEXT,
                    snapshot_json TEXT,
                    PRIMARY KEY (experiment_id, node_id)
                );
                CREATE TABLE IF NOT EXISTS export_state (
                    singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
                    path TEXT NOT NULL,
                    byte_offset INTEGER NOT NULL
                );
            """)
            self._experiments = {}
            for row in self._db.execute("SELECT registration_json FROM experiments"):
                registration = ExperimentRegistration.model_validate_json(row["registration_json"])
                self._remember_registration(registration)
            self._recover_export()
        except BaseException:
            self.close()
            raise

    def _remember_registration(self, registration: ExperimentRegistration) -> None:
        registration = registration.model_copy(deep=True)
        self._experiments[registration.experiment_id] = StoredExperiment(
            registration, {node.node_id: node for node in registration.nodes}
        )

    def register(self, registration: ExperimentRegistration, *, allow_existing: bool = False) -> None:
        with self._lock:
            existing = self._experiments.get(registration.experiment_id)
            if existing is not None:
                if allow_existing and existing.registration == registration:
                    return
                raise ExperimentAlreadyExists(registration.experiment_id)
            with self._db:
                self._db.execute(
                    "INSERT INTO experiments (experiment_id, registration_json) VALUES (?, ?)",
                    (registration.experiment_id, registration.model_dump_json()),
                )
                self._db.executemany(
                    "INSERT INTO node_state (experiment_id, node_id) VALUES (?, ?)",
                    ((registration.experiment_id, node.node_id) for node in registration.nodes),
                )
            self._remember_registration(registration)

    def _experiment(self, experiment_id: str) -> StoredExperiment:
        try:
            return self._experiments[experiment_id]
        except KeyError:
            raise UnknownExperiment(experiment_id) from None

    def submit(self, experiment_id: str, node_id: str, node_type: str, node_index: int,
               events: list[Event]) -> tuple[int, int, int | None]:
        with self._lock:
            experiment = self._experiment(experiment_id)
            node = experiment.nodes.get(node_id)
            if node is None or node.node_type != node_type or node.node_index != node_index:
                raise UnknownNode(node_id)
            accepted, duplicates, last_sequence = 0, 0, None
            records = []  # Only this request's accepted batch, never the history.
            export_offset = self._export_offset()
            self._db.execute("BEGIN IMMEDIATE")
            try:
                for event in events:
                    existing = self._db.execute(
                        "SELECT node_id, record_json FROM events WHERE experiment_id = ? AND event_id = ?",
                        (experiment_id, event.event_id),
                    ).fetchone()
                    if existing is not None:
                        stored_event = Event.model_validate(json.loads(existing["record_json"])["event"])
                        if existing["node_id"] != node_id or stored_event != event:
                            raise EventConflict(event.event_id)
                        duplicates += 1
                        continue
                    record = json.dumps({
                        "event": {"event_type": event.event_type, "event_id": event.event_id,
                                  "sequence": event.sequence, "occurred_at": event.occurred_at.isoformat(),
                                  "data": event.data},
                        "node": {"node_id": node_id, "node_type": node_type, "node_index": node_index},
                        "experiment_id": experiment_id,
                    })
                    self._db.execute(
                        "INSERT INTO events (experiment_id, event_id, node_id, record_json) VALUES (?, ?, ?, ?)",
                        (experiment_id, event.event_id, node_id, record),
                    )
                    self._update_current_state(experiment_id, node, event)
                    records.append(record)
                    accepted += 1
                    last_sequence = max(last_sequence if last_sequence is not None else 0, event.sequence)
                if accepted:
                    self._db.execute("UPDATE experiments SET event_count = event_count + ? WHERE experiment_id = ?",
                                     (accepted, experiment_id))
                    self._db.execute("UPDATE node_state SET has_events = 1 WHERE experiment_id = ? AND node_id = ?",
                                     (experiment_id, node_id))
                    if self._export_path is not None:
                        offset = self._append_exports(records, export_offset)
                        self._db.execute("UPDATE export_state SET byte_offset = ? WHERE singleton = 1", (offset,))
                self._db.commit()
            except BaseException:
                self._db.rollback()
                if self._export_path is not None:
                    # A failed batch is not accepted. Remove its partial export;
                    # if cleanup also fails, the next write/startup retries this.
                    try:
                        self._truncate_export(self._export_offset())
                    except (OSError, sqlite3.Error):
                        pass
                raise
            return accepted, duplicates, last_sequence

    def _update_current_state(self, experiment_id: str, node: ExperimentNode, event: Event) -> None:
        identity = (experiment_id, node.node_id)
        if event.event_type == EventType.NODE_STARTED:
            metadata = event.data.get("runtime_metadata")
            if metadata is None:
                address = event.data.get("hivemind_address", "")
                metadata = {"peer_id": address.rsplit("/p2p/", 1)[-1] if "/p2p/" in address else None,
                            "dht_id": None, "group_key": None, "num_replicas": None, "xor_distance": None,
                            "placement": "random", "focal_victim": False,
                            "storage_role": node.storage_role, "averager_role": node.averager_role,
                            "metadata_source": "manifest_and_standard_readiness_address"}
            self._db.execute("UPDATE node_state SET started = 1, runtime_metadata_json = ? "
                             "WHERE experiment_id = ? AND node_id = ?", (json.dumps(metadata), *identity))
        elif event.event_type == EventType.NODE_FINISHED:
            self._db.execute("UPDATE node_state SET finished = 1 WHERE experiment_id = ? AND node_id = ?", identity)
        elif event.event_type == EventType.EVAL_METRICS:
            self._db.execute("UPDATE node_state SET latest_eval_round = MAX(COALESCE(latest_eval_round, 0), ?) "
                             "WHERE experiment_id = ? AND node_id = ?", (event.data["round"], *identity))
        elif event.event_type == EventType.ECLIPSE_OBSERVATION and event.data.get("kind") == "storage_snapshot":
            self._db.execute("UPDATE node_state SET snapshot_json = ? WHERE experiment_id = ? AND node_id = ?",
                             (json.dumps(event.data), *identity))

    def start_barrier(self, experiment_id: str, node_id: str) -> tuple[str, list[str], list[str]]:
        with self._lock:
            experiment = self._experiment(experiment_id)
            asking = experiment.nodes.get(node_id)
            if asking is None:
                raise UnknownNode(node_id)
            started = {row["node_id"] for row in self._db.execute(
                "SELECT node_id FROM node_state WHERE experiment_id = ? AND started = 1", (experiment_id,))}
            group = [node.node_id for node in experiment.registration.nodes if node.start_group == asking.start_group]
            return asking.start_group, [node for node in group if node in started], [node for node in group if node not in started]

    def summary(self, experiment_id: str) -> tuple[int, int]:
        with self._lock:
            self._experiment(experiment_id)
            count = self._db.execute("SELECT event_count FROM experiments WHERE experiment_id = ?",
                                     (experiment_id,)).fetchone()[0]
            nodes = self._db.execute("SELECT COUNT(*) FROM node_state WHERE experiment_id = ? AND has_events = 1",
                                     (experiment_id,)).fetchone()[0]
            return count, nodes

    def runtime(self, experiment_id: str) -> dict:
        with self._lock:
            experiment = self._experiment(experiment_id)
            states = {row["node_id"]: row for row in self._db.execute(
                "SELECT * FROM node_state WHERE experiment_id = ?", (experiment_id,))}
            nodes, snapshots = {}, {}
            for node in experiment.registration.nodes:
                state = states[node.node_id]
                nodes[node.node_id] = {"node_id": node.node_id, "started": bool(state["started"]),
                                      "finished": bool(state["finished"]), "latest_eval_round": state["latest_eval_round"],
                                      "runtime_metadata": json.loads(state["runtime_metadata_json"])
                                                          if state["runtime_metadata_json"] else None,
                                      "storage_role": node.storage_role, "averager_role": node.averager_role}
                if state["snapshot_json"]:
                    snapshots[node.node_id] = json.loads(state["snapshot_json"])
            group_keys = {row["runtime_metadata"]["group_key"] for row in nodes.values()
                          if row["runtime_metadata"] and row["runtime_metadata"].get("group_key")}
            now = time.time()
            storage = {key: holder_metrics({node: sample for node, sample in snapshots.items() if sample["group_key"] == key},
                                          list(nodes), now, max_age=5.0) for key in group_keys}
            return {"experiment_id": experiment_id, "rounds": experiment.registration.rounds,
                    "nodes": list(nodes.values()), "storage_control": storage}

    def _export_offset(self) -> int:
        if self._export_path is None:
            return 0
        return self._db.execute("SELECT byte_offset FROM export_state WHERE singleton = 1").fetchone()[0]

    def _append_exports(self, records: list[str], offset: int) -> int:
        with self._export_path.open("r+b") as stream:
            stream.seek(offset)
            stream.truncate()
            for record in records:
                stream.write((record + "\n").encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
            return stream.tell()

    def _truncate_export(self, offset: int) -> None:
        with self._export_path.open("r+b") as stream:
            stream.truncate(offset)

    def _recover_export(self) -> None:
        """Discard uncommitted tails, or stream committed records into a lost export."""
        if self._export_path is None:
            return
        self._export_path.parent.mkdir(parents=True, exist_ok=True)
        state = self._db.execute("SELECT path, byte_offset FROM export_state WHERE singleton = 1").fetchone()
        if state is None:
            if self._export_path.exists() and self._export_path.stat().st_size:
                raise ValueError("Existing JSONL has no matching SQLite export state; use a new run output directory")
            with self._db:
                self._db.execute("INSERT INTO export_state VALUES (1, ?, -1)", (str(self._export_path),))
            offset = -1
        else:
            if state["path"] != str(self._export_path):
                raise ValueError("SQLite database belongs to a different JSONL export path")
            offset = state["byte_offset"]
        size = self._export_path.stat().st_size if self._export_path.exists() else -1
        if offset >= 0 and size >= offset:
            self._truncate_export(offset)
            return
        # Restart repair only. Normal runtime/status never scans this history.
        with self._export_path.open("wb") as stream:
            for row in self._db.execute("SELECT record_json FROM events ORDER BY ordinal"):
                stream.write((row[0] + "\n").encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
            offset = stream.tell()
        with self._db:
            self._db.execute("UPDATE export_state SET byte_offset = ? WHERE singleton = 1", (offset,))

    def close(self) -> None:
        with self._lock:
            if not self._closed:
                self._db.close()
                self._closed = True
                if self._temporary_directory is not None:
                    self._temporary_directory.cleanup()


# Preserve imports used by earlier integrations; this now always stores on disk.
InMemoryEventStore = SQLiteEventStore
