"""Minimal Hivemind DHT node runtime for experiment containers."""

from __future__ import annotations

import os
import signal
import socket
import time

import hivemind

LISTEN_PORT = 1337


def _initial_peers() -> list[str] | None:
    value = os.environ.get("INITIAL_PEERS", "").strip()
    return value.split() or None


def _container_ip() -> str:
    return socket.gethostbyname(socket.gethostname())


def main() -> None:
    initial_peers = _initial_peers()
    container_ip = _container_ip()
    dht = hivemind.DHT(
        initial_peers=initial_peers,
        host_maddrs=[f"/ip4/0.0.0.0/tcp/{LISTEN_PORT}"],
        announce_maddrs=[f"/ip4/{container_ip}/tcp/{LISTEN_PORT}"],
        start=True,
    )

    def shutdown(_signum: int, _frame: object) -> None:
        dht.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    visible_maddrs = dht.get_visible_maddrs()
    if not visible_maddrs:
        dht.shutdown()
        raise RuntimeError("Hivemind DHT started without a visible multiaddress")

    print(f"HIVEMIND_MADDR={visible_maddrs[0]}", flush=True)

    node_id = os.environ.get("NODE_ID", "unknown")
    print(f"HIVEMIND_READY=1 NODE_ID={node_id}", flush=True)

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
