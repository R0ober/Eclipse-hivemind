"""Shared DHT bootstrap and readiness helper, used by every node type."""

from __future__ import annotations

import os
import socket

import hivemind

LISTEN_PORT = 1337


def initial_peers() -> list[str]:
    value = os.environ.get("INITIAL_PEERS", "").strip()
    return value.split()


def container_ip() -> str:
    return socket.gethostbyname(socket.gethostname())


def start_dht() -> hivemind.DHT:
    """Start the DHT and print the readiness markers the orchestrator waits for."""
    ip = container_ip()
    dht = hivemind.DHT(
        initial_peers=initial_peers(),
        host_maddrs=[f"/ip4/0.0.0.0/tcp/{LISTEN_PORT}"],
        announce_maddrs=[f"/ip4/{ip}/tcp/{LISTEN_PORT}"],
        start=True,
    )

    visible_maddrs = dht.get_visible_maddrs()
    if not visible_maddrs:
        dht.shutdown()
        raise RuntimeError("Hivemind DHT started without a visible multiaddress")

    print(f"HIVEMIND_MADDR={visible_maddrs[0]}", flush=True)

    node_id = os.environ.get("NODE_ID", "unknown")
    print(f"HIVEMIND_READY=1 NODE_ID={node_id}", flush=True)

    return dht
