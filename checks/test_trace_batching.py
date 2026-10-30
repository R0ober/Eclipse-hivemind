"""Exercise real logging queues without a DHT, training or external HTTP."""
import contextlib
import io
import json
from pathlib import Path
import sys
import threading
import time
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src/eclipse_hivemind/nodes"))
from shared import eclipse_trace

IDENTITY = dict(experiment_id="batch-check", node_id="probe", node_type="probe", node_index=0)


class TraceBatchChecks(unittest.TestCase):
    def trace(self):
        trace = eclipse_trace.Trace(IDENTITY, "http://offline")
        trace.DRAIN_IDLE_TIMEOUT_SECONDS = 0.5
        trace.DRAIN_MAX_TIMEOUT_SECONDS = 2
        return trace

    def test_count_limit_preserves_every_payload_timestamp_and_fifo_order(self):
        trace = self.trace()
        trace.BATCH_SIZE = 7
        received, sizes = [], []
        with patch.object(eclipse_trace.time, "time", return_value=123456):
            for index in range(25):
                trace.emit("payload", index=index, value={"unicode": "Grüße", "nested": [False, None, 1.5]})
        def acknowledge(**kwargs):
            sizes.append(len(kwargs["observations"]))
            received.extend(kwargs["observations"])
            return {"accepted": len(kwargs["observations"]), "duplicates": 0}
        with patch.object(eclipse_trace.client, "send_eclipse_observations", side_effect=acknowledge):
            self.assertEqual(trace.close(), [])
        self.assertEqual(sizes, [7, 7, 7, 4])
        self.assertEqual([item["index"] for item in received], list(range(25)))
        for item in received:
            self.assertEqual(item["dht_time"], 123456)
            self.assertEqual(item["value"], {"unicode": "Grüße", "nested": [False, None, 1.5]})
        self.assertEqual(trace.acknowledged_observations, 25)

    def test_byte_limit_splits_batches_but_preserves_an_oversized_single_event(self):
        trace = self.trace()
        trace.BATCH_MAX_BYTES = 4096
        for index in range(5):
            trace.emit("bytes", index=index, payload="x" * 1000)
        trace.emit("oversized", payload="ä" * 10000)
        batches, wire_sizes = [], []
        def acknowledge(**kwargs):
            batches.append([event["data"] for event in kwargs["events"]])
            payload = {"schema_version": 1, "experiment_id": kwargs["experiment_id"],
                       "node": {key: kwargs[key] for key in ("node_id", "node_type", "node_index")},
                       "events": kwargs["events"]}
            wire_sizes.append(len(json.dumps(payload).encode("utf-8")))
            return {"accepted": len(kwargs["events"]), "duplicates": 0}
        with patch.object(eclipse_trace.client, "_post_events", side_effect=acknowledge):
            trace.close()
        self.assertEqual([item["index"] for batch in batches for item in batch if item["kind"] == "bytes"], list(range(5)))
        self.assertTrue(all(len(batch) <= 2 for batch in batches))
        self.assertEqual(batches[-1], [{"trace_schema_version": 2, "kind": "oversized",
                                      "dht_time": batches[-1][0]["dht_time"], "process_id": trace.owner_pid,
                                      "payload": "ä" * 10000}])
        self.assertEqual(trace.acknowledged_observations, 6)
        self.assertTrue(all(size <= trace.BATCH_MAX_BYTES for size in wire_sizes[:-1]))
        self.assertGreater(wire_sizes[-1], trace.BATCH_MAX_BYTES)

    def test_absolute_batch_age_flushes_even_when_the_queue_never_becomes_idle(self):
        trace = self.trace()
        trace.BATCH_SIZE = 10000
        trace.BATCH_MAX_BYTES = 100 * 1024 * 1024
        trace.BATCH_MAX_AGE_SECONDS = 0.005
        trace.BATCH_WAIT_SECONDS = 10
        for index in range(2000):
            trace.emit("continuous", index=index)
        batches = []
        def acknowledge(**kwargs):
            batches.append(list(kwargs["observations"]))
            return {"accepted": len(kwargs["observations"]), "duplicates": 0}
        with patch.object(eclipse_trace.client, "send_eclipse_observations", side_effect=acknowledge):
            trace.close()
        self.assertGreater(len(batches), 1)
        self.assertEqual([item["index"] for batch in batches for item in batch], list(range(2000)))

    def test_progressing_drain_has_its_own_budget_after_producer_waiting(self):
        trace = self.trace()
        trace.CLOSE_TIMEOUT_SECONDS = 0.025
        trace.BATCH_SIZE = 2
        trace.DRAIN_IDLE_TIMEOUT_SECONDS = 0.08
        trace.DRAIN_MAX_TIMEOUT_SECONDS = 1
        producer = Mock(pid=123)
        producer.join.side_effect = lambda timeout: time.sleep(0.02)
        producer.is_alive.return_value = False
        trace.register_process(producer)
        for index in range(10):
            trace.emit("slow", index=index)
        def acknowledge(**kwargs):
            time.sleep(0.02)
            return {"accepted": len(kwargs["observations"]), "duplicates": 0}
        with patch.object(eclipse_trace.client, "send_eclipse_observations", side_effect=acknowledge):
            trace.close()
        self.assertTrue(trace.drained)
        self.assertEqual(trace.acknowledged_observations, 10)
        producer.terminate.assert_not_called()

    def test_stalled_acknowledgement_fails_instead_of_claiming_complete_delivery(self):
        trace = self.trace()
        trace.DRAIN_IDLE_TIMEOUT_SECONDS = 0.03
        trace.BATCH_SIZE = 1
        trace.emit("stalled")
        release = threading.Event()
        def blocked(**kwargs):
            release.wait(1)
            return {"accepted": len(kwargs["observations"]), "duplicates": 0}
        with patch.object(eclipse_trace.client, "send_eclipse_observations", side_effect=blocked), \
                contextlib.redirect_stdout(io.StringIO()):
            try:
                with self.assertRaisesRegex(RuntimeError, "no acknowledged progress"):
                    trace.close()
                self.assertFalse(trace.drained)
            finally:
                release.set()
                trace.thread.join(1)

    def test_total_drain_limit_is_enforced_even_with_regular_acknowledgements(self):
        trace = self.trace()
        trace.BATCH_SIZE = 1
        trace.DRAIN_IDLE_TIMEOUT_SECONDS = 0.5
        trace.DRAIN_MAX_TIMEOUT_SECONDS = 0.04
        for index in range(30):
            trace.emit("slow", index=index)
        def acknowledge(**kwargs):
            time.sleep(0.01)
            return {"accepted": len(kwargs["observations"]), "duplicates": 0}
        with patch.object(eclipse_trace.client, "send_eclipse_observations", side_effect=acknowledge), \
                contextlib.redirect_stdout(io.StringIO()):
            try:
                with self.assertRaisesRegex(RuntimeError, "total drain limit"):
                    trace.close()
                self.assertGreater(trace.acknowledged_observations, 0)
                self.assertFalse(trace.drained)
            finally:
                trace.thread.join(1)
