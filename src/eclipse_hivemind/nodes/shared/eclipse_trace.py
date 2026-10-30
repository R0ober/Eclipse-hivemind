"""Move subprocess observations to the existing logger from the parent only."""

from __future__ import annotations

import json
import math
import multiprocessing
import queue
import os
import threading
import time

from . import client


def json_value(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {str(key): json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(item) for item in value]
    return value


class Trace:
    TRANSPORT_VERSION = "manager_queue_v3"
    BATCH_SIZE = 1000
    BATCH_MAX_BYTES = 1024 * 1024
    BATCH_MAX_AGE_SECONDS = 0.25
    BATCH_WAIT_SECONDS = 0.05
    # This waits for logging producers only, after Hivemind's own shutdown.
    CLOSE_TIMEOUT_SECONDS = 120
    DRAIN_IDLE_TIMEOUT_SECONDS = 120
    DRAIN_MAX_TIMEOUT_SECONDS = 600
    DRAIN_POLL_SECONDS = 0.1
    DRAIN_PROGRESS_LOG_SECONDS = 10

    def __init__(self, identity: dict, endpoint: str):
        self.identity, self.endpoint = identity, endpoint
        self.owner_pid = os.getpid()
        # The server owns the queue. Killing a Hivemind producer cannot leave
        # an inherited feeder lock or a partial frame in the consumer's pipe.
        self.manager = multiprocessing.get_context("fork").Manager()
        self.queue = self.manager.Queue()
        self.thread = None
        self.errors = []
        self.drained = False
        self.producers = set()
        self.finished_producers = set()
        self.processes = []
        self._progress_lock = threading.Lock()
        self.acknowledged_observations = 0

    def register_process(self, process):
        """Keep an owner-side handle; only wait after Hivemind's own shutdown."""
        self.processes.append(process)
        return process

    def _put(self, item):
        try:
            self.queue.put(item)
        except Exception as error:
            # Observation failure must not change a good peer's protocol result.
            self.errors.append(str(error))
            print("ECLIPSE_TELEMETRY_ERROR=transport " + str(error), flush=True)
            if item is not None:
                print("ECLIPSE_OBSERVATION=" + json.dumps(item, allow_nan=False), flush=True)

    def producer_started(self):
        self._put({"_trace_transport": "started", "process_id": os.getpid()})

    def producer_finished(self):
        self._put({"_trace_transport": "finished", "process_id": os.getpid(),
                   "delivery_errors": len(self.errors)})

    def emit(self, kind: str, **data):
        self._put(json_value({"trace_schema_version": 2, "kind": kind,
                              "dht_time": time.time(), "process_id": os.getpid(), **data}))

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._forward, daemon=True)
            self.thread.start()

    def _forward(self):
        batch = []
        batch_started = None
        # Conservative allowance for the request and per-event wrappers that
        # client.send_eclipse_observations adds. Oversized single observations
        # are sent alone, never truncated or discarded.
        request_bytes = len(json.dumps(self.identity).encode("utf-8")) + 512
        batch_bytes = request_bytes

        def flush():
            nonlocal batch, batch_started, batch_bytes
            self._send_batch(batch)
            batch, batch_started, batch_bytes = [], None, request_bytes

        while True:
            timeout = None
            if batch:
                remaining = self.BATCH_MAX_AGE_SECONDS - (time.monotonic() - batch_started)
                if remaining <= 0:
                    flush()
                    continue
                timeout = min(self.BATCH_WAIT_SECONDS, remaining)
            try:
                item = self.queue.get(timeout=timeout)
            except queue.Empty:
                flush()
                continue
            except Exception as error:
                self.errors.append(f"log queue unavailable: {error}")
                return
            if item is None:
                flush()
                self.drained = True
                return
            self.producers.add(item["process_id"])
            if item.get("_trace_transport") == "started":
                continue
            if item.get("_trace_transport") == "finished":
                self.finished_producers.add(item["process_id"])
                if item["delivery_errors"]:
                    self.errors.append(f"producer {item['process_id']} reported delivery errors")
                continue
            item_bytes = len(json.dumps(item, allow_nan=False).encode("utf-8")) + 512
            if batch and batch_bytes + item_bytes > self.BATCH_MAX_BYTES:
                flush()
            if not batch:
                batch_started = time.monotonic()
            batch.append(item)
            batch_bytes += item_bytes
            if len(batch) >= self.BATCH_SIZE or batch_bytes >= self.BATCH_MAX_BYTES:
                flush()

    def _send_batch(self, batch):
        if not batch:
            return
        try:
            acknowledgement = client.send_eclipse_observations(
                logger_endpoint=self.endpoint,
                **self.identity,
                observations=batch,
            )
            acknowledged = acknowledgement.get("accepted", 0) + acknowledgement.get("duplicates", 0)
            if acknowledged != len(batch):
                raise RuntimeError(f"logger acknowledged {acknowledged}/{len(batch)} observations")
            with self._progress_lock:
                self.acknowledged_observations += acknowledged
        except Exception as error:
            self.errors.append(str(error))
            # Preserve a local diagnostic copy only when the batch could not be persisted remotely.
            for item in batch:
                print("ECLIPSE_OBSERVATION=" + json.dumps(item, allow_nan=False), flush=True)
            print(f"ECLIPSE_TELEMETRY_ERROR=batch_size:{len(batch)} error:{error}", flush=True)

    def _wait_for_drain(self):
        started = last_progress = last_report = time.monotonic()
        with self._progress_lock:
            previous = self.acknowledged_observations
        while self.thread.is_alive():
            now = time.monotonic()
            with self._progress_lock:
                acknowledged = self.acknowledged_observations
            if acknowledged > previous:
                last_progress, previous = now, acknowledged
            remaining_total = self.DRAIN_MAX_TIMEOUT_SECONDS - (now - started)
            remaining_idle = self.DRAIN_IDLE_TIMEOUT_SECONDS - (now - last_progress)
            if remaining_total <= 0 or remaining_idle <= 0:
                reason = "total drain limit reached" if remaining_total <= 0 else "no acknowledged progress"
                raise RuntimeError(f"Eclipse telemetry did not drain before shutdown: {reason}; "
                                   f"acknowledged={acknowledged}; inspect container logs")
            if now - last_report >= self.DRAIN_PROGRESS_LOG_SECONDS:
                print(f"ECLIPSE_TELEMETRY_DRAIN=acknowledged:{acknowledged} elapsed:{now-started:.1f}s", flush=True)
                last_report = now
            self.thread.join(timeout=min(self.DRAIN_POLL_SECONDS, remaining_total, remaining_idle))

    def close(self):
        if os.getpid() != self.owner_pid:
            raise RuntimeError("only the observation owner may close the log transport")
        try:
            self.start()
            deadline = time.monotonic() + self.CLOSE_TIMEOUT_SECONDS
            for process in self.processes:
                if process.pid is not None:
                    process.join(timeout=max(0, deadline - time.monotonic()))
                    if process.is_alive():
                        raise RuntimeError(f"Eclipse log producer {process.pid} is still running after shutdown")
            self.producer_finished()
            self.queue.put(None)
            self._wait_for_drain()
            missing = sorted(self.producers - self.finished_producers)
            if missing and self.drained and not self.errors:
                # A killed producer cannot confirm its final observation. Persist
                # that uncertainty separately from delivery of received records.
                self._send_batch([{"trace_schema_version": 2, "kind": "telemetry_warning",
                                   "dht_time": time.time(), "process_id": self.owner_pid,
                                   "reason": "producer_end_unconfirmed", "missing_producers": missing}])
            if self.errors:
                raise RuntimeError(f"{len(self.errors)} Eclipse observations failed delivery; inspect container logs")
            if not self.drained:
                raise RuntimeError("Eclipse telemetry consumer stopped before draining the queue")
            return missing
        finally:
            # This is our logging helper only, never a Hivemind process.
            self.manager.shutdown()
