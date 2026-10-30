"""Isolated log-transport probes; no Hivemind network or training is started."""
from __future__ import annotations

import argparse
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import pickle
import struct
import sys
import threading
import time
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, "/app" if os.environ.get("DIT616_IMAGE_PROBE") else str(ROOT / "src/eclipse_hivemind/nodes"))
from shared import eclipse_trace

IDENTITY = dict(experiment_id="isolated-transport-probe", node_id="probe",
                node_type="probe", node_index=0)


def writer(trace, count, size, label):
    if hasattr(trace, "producer_started"):
        trace.producer_started()
    for index in range(count):
        trace.emit("probe", label=label, index=index, payload="x" * size)
    if hasattr(trace, "producer_finished"):
        trace.producer_finished()


def partial_writer(trace, ready):
    trace.producer_started()
    trace.emit("before_kill", index=0)
    # Deliberately truncate one real manager RPC frame. Private internals are
    # used only in this fault injector, never in the runtime implementation.
    connection = trace.queue._tls.connection
    request = (trace.queue._id, "put", ({"process_id": os.getpid(), "kind": "partial",
                                       "payload": "x" * 262144},), {})
    payload = pickle.dumps(request)
    connection._send(struct.pack("!i", len(payload)) + payload[:1024])
    ready.set()
    time.sleep(30)


def legacy_partial_writer(queue, ready):
    # The old queue's shared write lock survives the killed writer. Its reader
    # also has an incomplete frame, reproducing the proposed deadlock mechanism.
    queue._wlock.acquire()
    payload = pickle.dumps({"payload": "x" * 262144})
    queue._writer._send(struct.pack("!i", len(payload)) + payload[:1024])
    ready.set()
    time.sleep(30)


def legacy_corruption():
    context = multiprocessing.get_context("fork")
    queue, ready = context.Queue(), context.Event()
    process = context.Process(target=legacy_partial_writer, args=(queue, ready))
    process.start()
    if not ready.wait(5):
        process.kill(); process.join(2)
        raise RuntimeError("fault producer did not start")
    consumer = threading.Thread(target=queue.get, daemon=True)
    consumer.start()
    process.terminate(); process.join(3)
    print("legacy_writer_terminated", flush=True)
    queue.put(None)
    queue.close()
    queue.join_thread()  # Expected to hang; caller imposes a strict outer timeout.
    raise RuntimeError("legacy queue unexpectedly finished")


def managed_corruption(fail_warning=False, live_producer=False):
    context = multiprocessing.get_context("fork")
    trace = eclipse_trace.Trace(IDENTITY, "http://unused.invalid")
    received = []
    def acknowledge(**kwargs):
        received.extend(item["kind"] for item in kwargs["observations"])
        if fail_warning and kwargs["observations"][0]["kind"] == "telemetry_warning":
            return {"accepted": 0, "duplicates": 0}
        return {"accepted": len(kwargs["observations"]), "duplicates": 0}
    with patch.object(eclipse_trace.client, "send_eclipse_observations", side_effect=acknowledge):
        ready = context.Event()
        process = context.Process(target=partial_writer, args=(trace, ready))
        process.start()
        trace.register_process(process)
        if not ready.wait(5):
            process.kill(); process.join(2); trace.manager.shutdown()
            raise RuntimeError("fault producer did not start")
        if live_producer:
            trace.CLOSE_TIMEOUT_SECONDS = 0.1
        else:
            process.terminate()  # Trace.close must wait for actual process exit.
        trace.emit("after_kill")
        trace.start()
        started = time.monotonic()
        try:
            missing = trace.close()
        except RuntimeError as error:
            assert fail_warning or live_producer, error
            result = dict(probe="managed-corruption", measurement_status="failed",
                          reason=str(error), seconds_to_close=time.monotonic()-started,
                          received=received, helper_alive=trace.manager._process.is_alive(),
                          producer_alive=process.is_alive())
        else:
            assert not fail_warning and not live_producer
            assert missing == [process.pid], missing
            assert trace.drained and not process.is_alive()
            result = dict(probe="managed-corruption", measurement_status="completed-with-warning",
                          seconds_to_close=time.monotonic()-started, received=received,
                          missing_producers=missing, helper_alive=trace.manager._process.is_alive())
    if live_producer:
        assert result["producer_alive"]  # Logging must not kill a Hivemind producer.
        process.terminate(); process.join(3)
    else:
        assert received == ["before_kill", "after_kill", "telemetry_warning"], received
    assert not result["helper_alive"]
    print(json.dumps(result), flush=True)


def memory(pid=None):
    if pid is None:
        path = Path("/sys/fs/cgroup/memory.current")
        return int(path.read_text()) if path.exists() else None
    path = Path(f"/proc/{pid}/smaps_rollup")
    if not path.exists():
        return None
    values = {}
    for line in path.read_text().splitlines():
        parts = line.split()
        if len(parts) == 3 and parts[2] == "kB":
            values[parts[0].rstrip(":")] = int(parts[1])*1024
    return {key: values.get(key) for key in ("Rss", "Pss", "Private_Clean", "Private_Dirty")}


def benchmark(args):
    if args.preload_torch:
        import torch
        torch.set_num_threads(1)
    module = eclipse_trace
    if args.legacy_source:
        spec = importlib.util.spec_from_file_location("shared.trace_checkpoint", args.legacy_source)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
    before = memory()
    trace = module.Trace(IDENTITY, "http://unused.invalid")
    after_create = memory()
    received, batches = 0, 0
    sequences = {}
    def acknowledge(**kwargs):
        nonlocal received, batches
        for item in kwargs["observations"]:
            assert item["trace_schema_version"] == 2
            assert len(item["payload"]) == args.payload_bytes
            sequences.setdefault(item["label"], []).append(item["index"])
        received += len(kwargs["observations"])
        batches += 1
        return {"accepted": len(kwargs["observations"]), "duplicates": 0}
    context = multiprocessing.get_context("fork")
    processes = [context.Process(target=writer, args=(trace, args.count, args.payload_bytes, label))
                 for label in range(args.producers)]
    with patch.object(module.client, "send_eclipse_observations", side_effect=acknowledge):
        started = time.monotonic()
        for process in processes:
            process.start()
        trace.start()
        for process in processes:
            process.join(30)
            if process.is_alive():
                process.kill(); process.join(2)
                if hasattr(trace, "manager"): trace.manager.shutdown()
                raise RuntimeError("benchmark producer timed out")
            assert process.exitcode == 0
        after_traffic = memory()
        helper_memory = memory(trace.manager._process.pid) if hasattr(trace, "manager") else None
        trace.close()
        seconds = time.monotonic()-started
    assert received == args.count*args.producers
    assert all(values == list(range(args.count)) for values in sequences.values())
    print(json.dumps(dict(probe="benchmark", transport="legacy" if args.legacy_source else "managed",
                          python=sys.version.split()[0], preload_torch=args.preload_torch,
                          observations=received, payload_bytes=args.payload_bytes,
                          producers=args.producers, batches=batches, seconds=seconds,
                          observations_per_second=received/seconds,
                          cgroup_before=before, cgroup_after_create=after_create,
                          cgroup_after_traffic=after_traffic, helper_memory=helper_memory,
                          note="Synthetic acknowledged batches; no HTTP, DHT or training workload.")), flush=True)


def metadata():
    import hashlib
    import importlib.metadata
    import inspect
    from hivemind import DHT
    files = {name: hashlib.sha256((Path('/app/shared')/name).read_bytes()).hexdigest()
             for name in ('eclipse_trace.py', 'eclipse_dht.py', 'eclipse_averaging.py', 'eclipse_runtime.py')}
    versions = {package.metadata['Name']: package.version for package in importlib.metadata.distributions()}
    print(json.dumps(dict(probe='image-metadata', python=sys.version.split()[0],
                          packages=versions, source_sha256=files,
                          transport=eclipse_trace.Trace.TRANSPORT_VERSION,
                          dht_shutdown_default=inspect.signature(DHT.__init__).parameters['shutdown_timeout'].default)), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("legacy-corruption", "managed-corruption", "managed-warning-failure",
                                          "live-producer", "benchmark", "metadata"), required=True)
    parser.add_argument("--count", type=int, default=1000)
    parser.add_argument("--payload-bytes", type=int, default=4096)
    parser.add_argument("--producers", type=int, default=2)
    parser.add_argument("--preload-torch", action="store_true")
    parser.add_argument("--legacy-source", type=Path)
    options = parser.parse_args()
    if options.mode == "legacy-corruption": legacy_corruption()
    elif options.mode == "managed-corruption": managed_corruption()
    elif options.mode == "managed-warning-failure": managed_corruption(fail_warning=True)
    elif options.mode == "live-producer": managed_corruption(live_producer=True)
    elif options.mode == "metadata": metadata()
    else: benchmark(options)
