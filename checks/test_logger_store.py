"""Collector persistence, retry, concurrent ingestion and bounded-history checks.

Uses the real Pydantic models and SQLite, but starts no containers or swarm.
Run with the project's host environment: python -m unittest discover -s checks.
"""

from __future__ import annotations

import asyncio
import copy
import gc
import json
import sqlite3
import sys
import tempfile
import tracemalloc
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from eclipse_hivemind.logger.models import Event, EventType, ExperimentRegistration
from eclipse_hivemind.logger.store import (
    EventConflict, ExperimentAlreadyExists, SQLiteEventStore, UnknownExperiment, UnknownNode,
)


def registration(experiment_id="test"):
    return ExperimentRegistration(experiment_id=experiment_id, experiment_name="collector-test", rounds=5,
                                  nodes=[{"node_id": "victim-0", "node_type": "victim", "node_index": 0,
                                          "start_group": "first", "storage_role": "honest", "averager_role": "honest"},
                                         {"node_id": "storage-0", "node_type": "storage", "node_index": 0,
                                          "start_group": "second", "storage_role": "malicious", "averager_role": "disabled"}])


def event(event_id, kind=EventType.TRAINING_METRICS, *, sequence=0, data=None):
    return Event(event_id=event_id, sequence=sequence, event_type=kind,
                 occurred_at=datetime(2026, 10, 2, tzinfo=timezone.utc), data=data or {})


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.export = Path(self.directory.name) / "events.jsonl"
        self.store = self.open_store()
        self.store.register(registration())

    def open_store(self):
        store = SQLiteEventStore(self.export)
        self.addCleanup(store.close)
        return store

    def submit(self, events, *, node="victim-0", kind="victim", experiment="test", store=None):
        return (store or self.store).submit(experiment, node, kind, 0, events)

    def raw_records(self):
        return [json.loads(line) for line in self.export.read_text().splitlines()]

    def test_controlled_averager_registration_survives_persistence(self):
        registered = registration("controlled-test")
        registered.nodes[1] = registered.nodes[1].model_copy(update={"averager_role": "controlled"})
        registered = ExperimentRegistration.model_validate(registered.model_dump())
        self.store.register(registered)
        self.store.submit("controlled-test", "storage-0", "storage", 0,
                          [event("controlled-start", EventType.NODE_STARTED,
                                 data={"hivemind_address": "/ip4/127.0.0.1/tcp/1000/p2p/controlled-peer"})])
        self.store.close()
        recovered = self.open_store().runtime("controlled-test")
        storage = next(node for node in recovered["nodes"] if node["node_id"] == "storage-0")
        self.assertEqual(storage["runtime_metadata"]["averager_role"], "controlled")
        self.assertEqual(storage["runtime_metadata"]["storage_role"], "malicious")

    def test_duplicates_conflicts_and_event_identity_are_preserved(self):
        first = event("same", sequence=9, data={"nested": [1, {"value": True}], "text": "α"})
        self.assertEqual(self.submit([first, first]), (1, 1, 9))
        self.assertEqual(self.submit([copy.deepcopy(first)]), (0, 1, None))
        equivalent = first.model_copy(update={"data": {"text": "α", "nested": [1, {"value": True}]}})
        self.assertEqual(self.submit([equivalent]), (0, 1, None))
        with self.assertRaises(EventConflict):
            self.submit([first.model_copy(update={"sequence": 10})])
        with self.assertRaises(EventConflict):
            self.submit([first], node="storage-0", kind="storage")
        self.assertEqual(self.store.summary("test"), (1, 1))
        self.assertEqual(self.raw_records()[0]["event"], {
            "event_type": "training_metrics", "event_id": "same", "sequence": 9,
            "occurred_at": first.occurred_at.isoformat(), "data": first.data,
        })

    def test_conflict_rolls_back_entire_batch_and_current_state(self):
        original = event("exists")
        self.submit([original])
        baseline = self.export.read_bytes()
        with self.assertRaises(EventConflict):
            self.submit([event("start", EventType.NODE_STARTED), original.model_copy(update={"data": {"different": 1}})])
        self.assertEqual(self.export.read_bytes(), baseline)
        self.assertEqual(self.store.summary("test"), (1, 1))
        self.assertFalse(self.store.runtime("test")["nodes"][0]["started"])
        self.assertEqual(self.submit([event("start", EventType.NODE_STARTED)]), (1, 0, 0))

    def test_export_failure_rolls_back_and_retry_exports_once(self):
        self.submit([event("prior")])
        baseline = self.export.read_bytes()
        pending = event("start", EventType.NODE_STARTED, sequence=1)
        with patch("eclipse_hivemind.logger.store.os.fsync", side_effect=OSError("disk failure")):
            with self.assertRaises(OSError):
                self.submit([pending])
        self.assertEqual(self.export.read_bytes(), baseline)
        self.assertEqual(self.store.summary("test"), (1, 1))
        self.assertFalse(self.store.runtime("test")["nodes"][0]["started"])
        self.assertEqual(self.submit([pending]), (1, 0, 1))
        self.assertEqual(self.submit([pending]), (0, 1, None))
        self.assertEqual([row["event"]["event_id"] for row in self.raw_records()], ["prior", "start"])

    def test_lost_acknowledgement_after_commit_does_not_truncate_committed_export(self):
        connection = self.store._db

        class InterruptedCommit:
            def __getattr__(self, name):
                return getattr(connection, name)

            def commit(self):
                connection.commit()
                raise KeyboardInterrupt("interrupted after commit before acknowledgement")

        original = event("committed", EventType.NODE_STARTED)
        self.store._db = InterruptedCommit()
        try:
            with self.assertRaises(KeyboardInterrupt):
                self.submit([original])
        finally:
            self.store._db = connection
        self.assertEqual(self.store.summary("test"), (1, 1))
        self.assertTrue(self.store.runtime("test")["nodes"][0]["started"])
        self.assertEqual(self.submit([original]), (0, 1, None))
        self.assertEqual([row["event"]["event_id"] for row in self.raw_records()], ["committed"])

    def test_current_lifecycle_snapshot_and_stale_coverage_match_history_semantics(self):
        now = 1000.0
        metadata = {"peer_id": "victim-peer", "group_key": "key", "storage_role": "honest", "averager_role": "honest"}
        snapshot = {"kind": "storage_snapshot", "group_key": "key", "dht_time": now,
                    "storage_role": "honest", "replica_records": [
                        {"peer_id": "candidate", "active": True, "expiration_time": now + 100}], "cache_records": []}
        self.submit([event("start", EventType.NODE_STARTED, data={"runtime_metadata": metadata}),
                     event("eval-3", EventType.EVAL_METRICS, data={"round": 3}),
                     event("eval-1", EventType.EVAL_METRICS, data={"round": 1}),
                     event("snapshot", EventType.ECLIPSE_OBSERVATION, data=snapshot),
                     event("finished", EventType.NODE_FINISHED)])
        self.submit([event("storage-start", EventType.NODE_STARTED,
                           data={"hivemind_address": "/ip4/127.0.0.1/tcp/1000/p2p/storage-peer"}),
                     event("storage-snapshot", EventType.ECLIPSE_OBSERVATION,
                           data={**snapshot, "storage_role": "malicious"})], node="storage-0", kind="storage")
        with patch("eclipse_hivemind.logger.store.time.time", return_value=now):
            state = self.store.runtime("test")
            victim, storage = state["nodes"]
            self.assertEqual((victim["started"], victim["finished"], victim["latest_eval_round"]), (True, True, 3))
            self.assertEqual(storage["runtime_metadata"]["peer_id"], "storage-peer")
            record = state["storage_control"]["key"]["records"][0]
            self.assertEqual((record["identified_holders"], record["storage_control_share"]), (2, 0.5))
            # Mutating an API response cannot corrupt the cached current state.
            victim["runtime_metadata"]["peer_id"] = "changed"
            self.assertEqual(self.store.runtime("test")["nodes"][0]["runtime_metadata"], metadata)
        with patch("eclipse_hivemind.logger.store.time.time", return_value=now + 6):
            stale = self.store.runtime("test")["storage_control"]["key"]
            self.assertFalse(stale["complete_coverage"])
            self.assertEqual(stale["missing_or_stale_nodes"], ["victim-0", "storage-0"])
        self.assertEqual(self.store.start_barrier("test", "victim-0"), ("first", ["victim-0"], []))
        self.assertEqual(self.store.start_barrier("test", "storage-0"), ("second", ["storage-0"], []))

    def test_latest_received_snapshot_replaces_previous_and_zero_eval_is_kept(self):
        self.submit([event("start", EventType.NODE_STARTED, data={"runtime_metadata": {"group_key": "key"}}),
                     event("zero", EventType.EVAL_METRICS, data={"round": 0})])
        for index, now in enumerate([1000, 999]):
            self.submit([event(str(index), EventType.ECLIPSE_OBSERVATION, data={
                "kind": "storage_snapshot", "group_key": "key", "dht_time": now,
                "storage_role": "honest", "replica_records": [], "cache_records": []})])
        with patch("eclipse_hivemind.logger.store.time.time", return_value=1004.5):
            state = self.store.runtime("test")
            self.assertEqual(state["nodes"][0]["latest_eval_round"], 0)
            self.assertEqual(state["storage_control"]["key"]["observed_nodes"], [])

    def test_restart_restores_state_and_dedup_without_replaying_history(self):
        original = event("start", EventType.NODE_STARTED)
        self.submit([original, event("eval", EventType.EVAL_METRICS, data={"round": 2})])
        before = self.store.runtime("test")
        self.store.close()
        reopened = self.open_store()
        reopened.register(registration(), allow_existing=True)
        self.assertEqual(reopened.runtime("test"), before)
        self.assertEqual(reopened.summary("test"), (2, 1))
        self.assertEqual(self.submit([original], store=reopened), (0, 1, None))
        with self.assertRaises(ExperimentAlreadyExists):
            reopened.register(registration())
        with self.assertRaises(ExperimentAlreadyExists):
            reopened.register(registration().model_copy(update={"rounds": 6}), allow_existing=True)
        self.assertEqual(len(self.raw_records()), 2)

    def test_restart_discards_uncommitted_export_tail(self):
        self.submit([event("kept")])
        baseline = self.export.read_bytes()
        self.store.close()
        with self.export.open("ab") as stream:
            stream.write(b'{"partial uncommitted batch"')
        reopened = self.open_store()
        self.assertEqual(self.export.read_bytes(), baseline)
        self.assertEqual(reopened.summary("test"), (1, 1))

    def test_restart_rebuilds_missing_or_short_export_from_database(self):
        self.submit([event("one"), event("two", sequence=1)])
        baseline = self.export.read_bytes()
        self.store.close()
        self.export.write_bytes(baseline[:20])
        reopened = self.open_store()
        self.assertEqual(self.export.read_bytes(), baseline)
        reopened.close()
        self.export.unlink()
        self.open_store()
        self.assertEqual(self.export.read_bytes(), baseline)

    def test_existing_legacy_raw_file_is_never_overwritten(self):
        legacy = Path(self.directory.name) / "legacy.jsonl"
        original = b'{"historical": "measurement"}\n'
        legacy.write_bytes(original)
        with self.assertRaises(ValueError):
            SQLiteEventStore(legacy)
        self.assertEqual(legacy.read_bytes(), original)

    def test_parallel_same_event_is_accepted_and_exported_exactly_once(self):
        original = event("same", EventType.NODE_STARTED)
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda _: self.submit([original]), range(16)))
        self.assertEqual(sum(result[0] for result in results), 1)
        self.assertEqual(sum(result[1] for result in results), 15)
        self.assertEqual(len(self.raw_records()), 1)
        self.assertEqual(self.store.summary("test"), (1, 1))

    def test_parallel_distinct_events_keep_database_export_and_state_consistent(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            results = list(pool.map(lambda index: self.submit([
                event(str(index), EventType.EVAL_METRICS, sequence=index, data={"round": index})]), range(32)))
        self.assertEqual(sum(result[0] for result in results), 32)
        self.assertEqual(self.store.runtime("test")["nodes"][0]["latest_eval_round"], 31)
        self.assertEqual(len({row["event"]["event_id"] for row in self.raw_records()}), 32)

    def test_status_queries_do_not_read_event_history(self):
        self.submit([event("start", EventType.NODE_STARTED)])

        def deny_history_reads(action, table, _column, _database, _trigger):
            return sqlite3.SQLITE_DENY if action == sqlite3.SQLITE_READ and table == "events" else sqlite3.SQLITE_OK

        self.store._db.set_authorizer(deny_history_reads)
        self.assertEqual(self.store.summary("test"), (1, 1))
        self.assertEqual(self.store.start_barrier("test", "victim-0"), ("first", ["victim-0"], []))
        self.assertTrue(self.store.runtime("test")["nodes"][0]["started"])

    def test_large_history_does_not_accumulate_python_payload_objects(self):
        tracemalloc.start()
        self.addCleanup(tracemalloc.stop)
        gc.collect()
        before = tracemalloc.get_traced_memory()[0]
        for start in range(0, 2000, 100):
            self.submit([event(str(index), sequence=index, data={"blob": str(index) + "x" * 16384})
                         for index in range(start, start + 100)])
        gc.collect()
        retained = tracemalloc.get_traced_memory()[0] - before
        self.assertLess(retained, 2 * 1024 * 1024)
        self.assertEqual(self.store.summary("test"), (2000, 1))
        self.assertGreater(self.export.stat().st_size, 30 * 1024 * 1024)
        self.assertEqual(self.store._db.execute("PRAGMA cache_size").fetchone()[0], -4096)

    def test_identity_validation_and_experiment_namespacing(self):
        with self.assertRaises(UnknownExperiment):
            self.store.runtime("unknown")
        with self.assertRaises(UnknownNode):
            self.submit([event("bad")], kind="storage")
        with self.assertRaises(UnknownNode):
            self.store.start_barrier("test", "unknown")
        self.store.register(registration("other"))
        self.submit([event("shared-id")])
        self.submit([event("shared-id")], experiment="other")
        self.assertEqual(self.store.summary("test"), (1, 1))
        self.assertEqual(self.store.summary("other"), (1, 1))


class APITests(unittest.TestCase):
    def test_api_ingestion_retry_conflict_status_and_shutdown(self):
        from eclipse_hivemind.logger.app import create_app

        async def request(app, path, *, method="GET", payload=None):
            response = []

            async def receive():
                return {"type": "http.request", "body": json.dumps(payload).encode() if payload is not None else b"",
                        "more_body": False}

            async def send(message):
                response.append(message)

            await app({"type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1", "method": method,
                       "scheme": "http", "path": path.split("?")[0], "raw_path": path.split("?")[0].encode(),
                       "query_string": path.partition("?")[2].encode(), "root_path": "",
                       "headers": [(b"content-type", b"application/json")], "client": ("127.0.0.1", 1),
                       "server": ("127.0.0.1", 8080)}, receive, send)
            status = next(message["status"] for message in response if message["type"] == "http.response.start")
            body = b"".join(message.get("body", b"") for message in response if message["type"] == "http.response.body")
            return status, json.loads(body)

        async def scenario(store):
            app = create_app(store, registration())
            async with app.router.lifespan_context(app):
                base = "/api/v1/experiments/test"
                payload = {"experiment_id": "test", "node": {"node_id": "victim-0", "node_type": "victim", "node_index": 0},
                           "events": [event("start", EventType.NODE_STARTED).model_dump(mode="json")]}
                status, ack = await request(app, base + "/events", method="POST", payload=payload)
                self.assertEqual((status, ack["accepted"], ack["duplicates"]), (202, 1, 0))
                status, ack = await request(app, base + "/events", method="POST", payload=payload)
                self.assertEqual((status, ack["accepted"], ack["duplicates"]), (202, 0, 1))
                payload["events"][0]["sequence"] = 1
                status, _ = await request(app, base + "/events", method="POST", payload=payload)
                self.assertEqual(status, 409)
                self.assertEqual((await request(app, base + "/summary"))[1]["event_count"], 1)
                self.assertTrue((await request(app, base + "/start-barrier?node_id=victim-0"))[1]["ready"])
                self.assertTrue((await request(app, base + "/runtime"))[1]["nodes"][0]["started"])
            self.assertTrue(store._closed)

        with tempfile.TemporaryDirectory() as directory:
            store = SQLiteEventStore(Path(directory) / "events.jsonl")
            try:
                asyncio.run(scenario(store))
            finally:
                store.close()


if __name__ == "__main__":
    unittest.main()
