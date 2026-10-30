"""Append-only per-process spools; the parent alone sends HTTP batches.

No Manager RPC runs in the Hivemind event loop. An absent terminal marker still
fails: process exit is not an ACK.
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
import tempfile
import threading
import time

from shared import client


def safe(value):
    if isinstance(value, float) and not math.isfinite(value):
        return str(value)
    if isinstance(value, dict):
        return {str(k): safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [safe(v) for v in value]
    return value


class Trace:
    TRANSPORT_VERSION = "process_spool_v4"
    BATCH_SIZE = 1000
    BATCH_MAX_BYTES = 1024 * 1024
    CLOSE_TIMEOUT_SECONDS = 120
    DRAIN_IDLE_TIMEOUT_SECONDS = 120
    DRAIN_MAX_TIMEOUT_SECONDS = 1800

    def __init__(self, identity, endpoint):
        self.identity, self.endpoint = identity, endpoint
        self.owner_pid = os.getpid()
        self.directory = Path(tempfile.mkdtemp(prefix="eclipse-trace-"))
        self.errors, self.processes = [], []
        self.thread = None
        self.stop = threading.Event()
        self.readers, self.sequences, self.ended = {}, {}, set()
        self.acknowledged_observations = 0
        self.drained = False
        self.completed = False
        self.writer_pid = None

    def register_process(self, process):
        self.processes.append(process)
        return process

    def _write(self, item):
        pid = os.getpid()
        # A lock inherited from a multithreaded parent must never be acquired.
        if self.writer_pid != pid:
            self.writer_pid, self.sequence, self.finished = pid, 0, False
            self.write_lock = threading.Lock()
            self.writer = os.open(self.directory / f"{pid}.jsonl", os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        with self.write_lock:
            if self.finished:
                raise RuntimeError("observation after terminal producer marker")
            try:
                record = {**safe(item), "process_id": pid, "trace_sequence": self.sequence}
                payload = (json.dumps(record, allow_nan=False) + "\n").encode()
                # Handle short writes explicitly. A killed partial line stays invalid.
                view = memoryview(payload)
                while view:
                    count = os.write(self.writer, view)
                    if count <= 0:
                        raise OSError("trace spool made no write progress")
                    view = view[count:]
            except Exception as error:
                self.errors.append(f"producer write failed: {error}")
                raise
            self.sequence += 1
            if item.get("_trace_transport") == "finished":
                self.finished = True

    def producer_started(self):
        self._write({"_trace_transport": "started"})

    def producer_finished(self):
        if self.writer_pid == os.getpid() and self.finished:
            return
        self._write({"_trace_transport": "finished", "delivery_errors": len(self.errors)})

    def emit(self, kind, **data):
        self._write({"trace_schema_version": 2, "kind": kind, "dht_time": time.time(), **data})

    def start(self):
        if self.thread is None:
            self.thread = threading.Thread(target=self._forward, daemon=True)
            self.thread.start()

    def _send(self, batch):
        if not batch:
            return
        ack = client.send_eclipse_observations(**self.identity, logger_endpoint=self.endpoint, observations=batch)
        if ack.get("accepted", 0) + ack.get("duplicates", 0) != len(batch):
            raise RuntimeError("partial trace acknowledgement")
        self.acknowledged_observations += len(batch)

    def _forward(self):
        try:
            while True:
                # Only an EOF scan begun after all writers stop can prove that
                # the spools are drained. Stop may change during an idle scan.
                stopping = self.stop.is_set()
                progress = False
                for path in sorted(self.directory.glob("*.jsonl")):
                    pid = int(path.stem)
                    if pid not in self.readers:
                        self.readers[pid] = path.open("rb")
                    stream = self.readers[pid]
                    batch, size = [], 0
                    while len(batch) < self.BATCH_SIZE and size < self.BATCH_MAX_BYTES:
                        offset = stream.tell()
                        line = stream.readline()
                        if not line or not line.endswith(b"\n"):
                            stream.seek(offset)
                            if line and stopping:
                                raise RuntimeError(f"truncated producer spool: {pid}")
                            break
                        progress = True
                        item = json.loads(line)
                        if item.get("process_id") != pid or item.get("trace_sequence") != self.sequences.get(pid, 0):
                            raise RuntimeError(f"producer sequence mismatch: {pid}")
                        if pid in self.ended:
                            raise RuntimeError(f"record after producer end: {pid}")
                        self.sequences[pid] = item["trace_sequence"] + 1
                        if item.get("_trace_transport") == "finished":
                            self.ended.add(pid)
                            if item.get("delivery_errors"):
                                self.errors.append(f"producer {pid} reported write errors")
                        elif "_trace_transport" not in item:
                            batch.append(item)
                            size += len(line)
                    try:
                        self._send(batch)
                    except Exception:
                        for item in batch:
                            print("ECLIPSE_OBSERVATION=" + json.dumps(item), flush=True)
                        raise
                if stopping and not progress:
                    self.drained = True
                    return
                if not progress:
                    time.sleep(0.025)
        except Exception as error:
            self.errors.append(str(error))
            print("ECLIPSE_TELEMETRY_ERROR=" + str(error), flush=True)

    def close(self):
        if os.getpid() != self.owner_pid:
            raise RuntimeError("only the owner may close trace transport")
        self.start()
        deadline = time.monotonic() + self.CLOSE_TIMEOUT_SECONDS
        try:
            for process in self.processes:
                if process.pid is not None:
                    process.join(timeout=max(0, deadline - time.monotonic()))
                    if process.is_alive():
                        raise RuntimeError(f"live producer after native shutdown: {process.pid}")
            self.producer_finished()
            self.stop.set()
            began = last_progress = time.monotonic()
            next_report = began
            previous = self.acknowledged_observations
            while self.thread.is_alive():
                if self.acknowledged_observations != previous:
                    previous, last_progress = self.acknowledged_observations, time.monotonic()
                now = time.monotonic()
                if now >= next_report:
                    print(f"TELEMETRY_DRAIN elapsed={now-began:.0f}s "
                          f"acknowledged={self.acknowledged_observations} "
                          f"idle={now-last_progress:.1f}s "
                          f"budget={self.DRAIN_MAX_TIMEOUT_SECONDS}s", flush=True)
                    next_report = now + 30
                if now - began > self.DRAIN_MAX_TIMEOUT_SECONDS:
                    raise RuntimeError(f"trace drain total deadline exceeded: "
                                       f"elapsed={now-began:.1f}s, acknowledged={self.acknowledged_observations}")
                if now - last_progress > self.DRAIN_IDLE_TIMEOUT_SECONDS:
                    raise RuntimeError(f"trace drain stalled without acknowledged progress: "
                                       f"idle={now-last_progress:.1f}s, acknowledged={self.acknowledged_observations}")
                self.thread.join(0.1)
            expected = {self.owner_pid, *(p.pid for p in self.processes if p.pid is not None)}
            missing = (expected | self.sequences.keys()) - self.ended
            if self.errors or not self.drained or missing:
                raise RuntimeError(f"incomplete telemetry: errors={self.errors}, missing_producer_ends={sorted(missing)}")
            # Durable HTTP ACK for all received observations precedes this audit.
            self._send([{"trace_schema_version": 2, "kind": "telemetry_complete", "dht_time": time.time(),
                         "transport_version": self.TRANSPORT_VERSION, "process_id": self.owner_pid,
                         "producer_count": len(expected), "acknowledged_observations": self.acknowledged_observations,
                         "producer_record_counts": self.sequences}])
            self.completed = True
            return []
        finally:
            if self.writer_pid == self.owner_pid:
                os.close(self.writer)
            if not self.thread.is_alive():
                for stream in self.readers.values():
                    stream.close()
            # On failure keep spools for diagnosis until container cleanup.
            if self.completed:
                for path in self.directory.glob("*.jsonl"):
                    path.unlink()
                self.directory.rmdir()
            else:
                # Docker cleanup preserves stdout; retain even an unsent tail.
                for path in self.directory.glob("*.jsonl"):
                    with path.open(errors="replace") as stream:
                        for line in stream:
                            print("ECLIPSE_SPOOL=" + line.rstrip(), flush=True)
