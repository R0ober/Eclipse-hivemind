"""Compact instrumentation; native policy and full primary measurement payloads stay intact."""
from collections import Counter
import os
import threading
import time

from .trace_full import Trace as FullTrace
from .compact import CounterWindow, INTERVAL_SECONDS, SCHEMA
from shared import client


class Trace(FullTrace):
    TRANSPORT_VERSION = "process_spool_compact_v1"

    def __init__(self, identity, endpoint):
        super().__init__(identity, endpoint)
        self._compact_pid = None
        self._received_counts = Counter()
        self._received_summaries = 0
        self._window()

    def _window(self):
        pid = os.getpid()
        if self._compact_pid != pid:
            # Never use a lock/counter inherited from a multithreaded parent.
            self._compact_pid = pid
            self._compact_lock = threading.Lock()
            self._compact_window = CounterWindow(self.identity["node_id"])
            self._compact_flushed_at = time.monotonic()
        return self._compact_window

    def _flush_locked(self):
        payload = self._compact_window.pop()
        if payload is not None:
            FullTrace.emit(self, "telemetry_event_counts", **payload)
        self._compact_flushed_at = time.monotonic()

    def emit(self, kind, **data):
        window = self._window()
        with self._compact_lock:
            if self.writer_pid == os.getpid() and self.finished:
                raise RuntimeError("observation after terminal producer marker")
            aggregated = window.add(kind, data, time.time())
            if time.monotonic() - self._compact_flushed_at >= INTERVAL_SECONDS:
                self._flush_locked()
            if not aggregated:
                FullTrace.emit(self, kind, **data)

    def producer_finished(self):
        self._window()
        with self._compact_lock:
            if not (self.writer_pid == os.getpid() and self.finished):
                self._flush_locked()
            return super().producer_finished()

    def _send(self, batch):
        # FullTrace enforces acknowledged delivery and producer sequence/end checks.
        if len(batch) == 1 and batch[0].get("kind") == "telemetry_complete":
            batch = [{**batch[0], "compact_schema": SCHEMA,
                      "compact_summary_records": self._received_summaries,
                      "compacted_events_by_kind": dict(self._received_counts)}]
        super()._send(batch)
        for item in batch:
            if item.get("kind") == "telemetry_event_counts":
                self._received_summaries += 1
                self._received_counts.update(item["event_counts"])
