"""Exercise the installed DHT's IPC recovery and shutdown without a DHT network."""
from __future__ import annotations

import multiprocessing as mp
from pathlib import Path
import threading
from types import SimpleNamespace
from unittest.mock import Mock, patch

from hivemind.dht.dht import DHT
from hivemind.dht.node import DHTNode


class GatedConnection:
    def __init__(self, connection, gate, receive_errors, error_seen):
        self.connection, self.gate, self.receive_errors = connection, gate, receive_errors
        self.error_seen = error_seen

    def __getattr__(self, name):
        return getattr(self.connection, name)

    def recv(self):
        if not self.gate.wait(10):
            raise TimeoutError("IPC probe gate was not released")
        try:
            return self.connection.recv()
        except FileNotFoundError:
            with self.receive_errors.get_lock():
                self.receive_errors.value += 1
            self.error_seen.set()
            raise


class LocalDHTNode:
    """Keep the real DHT pipe/event loop; replace only the network participant."""
    def __init__(self, stopped):
        self.protocol = SimpleNamespace(wait_timeout=0.5)
        self.stopped = stopped

    async def shutdown(self):
        self.stopped.set()


def send_expiring_connection(connection):
    local, remote = mp.Pipe()
    try:
        connection.send(("_unused", [], {"descriptor": local}))
    finally:
        local.close()
        remote.close()


def check_shutdown(shutdown_timeout):
    context = mp.get_context("fork")
    gate, stopped, error_seen = context.Event(), context.Event(), context.Event()
    errors = context.Value("i", 0)

    async def create_node(*args, **kwargs):
        return LocalDHTNode(stopped)

    with patch.object(DHTNode, "create", side_effect=create_node):
        dht = DHT(start=False, shutdown_timeout=shutdown_timeout)
        dht._inner_pipe = GatedConnection(dht._inner_pipe, gate, errors, error_seen)
        sender = context.Process(target=send_expiring_connection, args=(dht._outer_pipe,))
        try:
            dht.run_in_background(timeout=10)
            sender.start()
            sender.join(10)
            if sender.is_alive() or sender.exitcode != 0:
                raise RuntimeError("IPC descriptor sender did not finish")
            # The sender's resource-sharer socket is now gone, while its
            # serialized descriptor is still waiting in the real DHT pipe.
            gate.set()
            if not error_seen.wait(5):
                raise RuntimeError("the installed DHT did not encounter the expired descriptor")
            dht.shutdown()
            dht.join(5)
            return dict(native_exit=dht.exitcode, receive_errors=errors.value,
                        node_shutdown_completed=stopped.is_set())
        finally:
            gate.set()
            for process in (sender, dht):
                if process.pid is not None:
                    if process.is_alive():
                        process.terminate()
                    process.join(5)


def check_terminal_drain():
    from factorial.trace import Trace

    trace = Trace(dict(experiment_id="shutdown-probe", node_id="probe", node_type="probe", node_index=0), "http://unused")
    trace.emit("first")
    idle, release = threading.Event(), threading.Event()
    original_open = Path.open
    received, errors = [], []

    class Reader:
        def __init__(self, stream):
            self.stream, self.empty_reads = stream, 0

        def __getattr__(self, name):
            return getattr(self.stream, name)

        def readline(self):
            line = self.stream.readline()
            if not line:
                self.empty_reads += 1
                if self.empty_reads == 2:
                    idle.set()
                    if not release.wait(5):
                        raise TimeoutError("terminal drain probe was not released")
            return line

    def open_stream(path, *args, **kwargs):
        stream = original_open(path, *args, **kwargs)
        return Reader(stream) if path.parent == trace.directory and args == ("rb",) else stream

    def close():
        try:
            trace.emit("last")
            trace.close()
        except BaseException as error:
            errors.append(str(error))

    with patch.object(Path, "open", open_stream), patch("factorial.trace.client.send_eclipse_observations",
            side_effect=lambda **kw: received.extend(kw["observations"]) or {"accepted": len(kw["observations"])}):
        trace.start()
        closer = threading.Thread(target=close)
        try:
            if not idle.wait(5):
                raise TimeoutError("terminal drain probe did not reach idle EOF")
            closer.start()
            if not trace.stop.wait(5):
                raise TimeoutError("terminal drain probe did not request stop")
        finally:
            release.set()
            if closer.ident is not None:
                closer.join(5)
        if (closer.is_alive() or errors or not trace.completed
                or [item["kind"] for item in received] != ["first", "last", "telemetry_complete"]):
            raise RuntimeError(f"installed terminal drain probe failed: {errors}")
    return dict(terminal_race_checked=True, acknowledged_observations=received[-1]["acknowledged_observations"])


def probe():
    terminal = check_terminal_drain()
    # These two controls deliberately send an expired descriptor. Their native
    # error is verified through the counter, without printing expected stacks.
    with patch("hivemind.dht.dht.logger", Mock(), create=True):
        short = check_shutdown(0.05)
        sufficient = check_shutdown(3)
    if (short["receive_errors"] != 1 or short["node_shutdown_completed"]
            or sufficient["receive_errors"] != 1 or sufficient["native_exit"] != 0
            or not sufficient["node_shutdown_completed"]):
        raise RuntimeError(f"installed DHT shutdown recovery failed: short={short}, sufficient={sufficient}")
    return dict(terminal_drain=terminal, short_grace=short, sufficient_grace=sufficient)
