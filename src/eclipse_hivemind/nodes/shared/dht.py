"""Shared DHT bootstrap and readiness helper, used by every node type."""

from __future__ import annotations

import os
import socket

import hivemind
from hivemind.dht.routing import DHTID

LISTEN_PORT = 1337


def initial_peers() -> list[str]:
    value = os.environ.get("INITIAL_PEERS", "").strip()
    return value.split()


def dht_id() -> DHTID | None:
    """The DHTID chosen by PARAM_DHT_ID_SOURCE, or None to let hivemind pick a random one."""
    source = os.environ.get("PARAM_DHT_ID_SOURCE")
    if not source:
        return None
    return DHTID.generate(source=source.encode())


async def _read_node_id(_dht: hivemind.DHT, node) -> DHTID:
    return node.node_id


def resolved_dht_id(dht: hivemind.DHT) -> str:
    """The DHTID the node is actually running with, as a 40-char hex string.

    Read back from the live node rather than recomputed, so it is recorded whether
    it came from PARAM_DHT_ID_SOURCE or was picked at random when the source is unset.
    """
    return f"{int(dht.run_coroutine(_read_node_id)):040x}"


def container_ip() -> str:
    return socket.gethostbyname(socket.gethostname())


def start_dht() -> hivemind.DHT:
    """Start the DHT and print the readiness markers the orchestrator waits for."""
    ip = container_ip()
    dht = hivemind.DHT(
        initial_peers=initial_peers(),
        host_maddrs=[f"/ip4/0.0.0.0/tcp/{LISTEN_PORT}"],
        announce_maddrs=[f"/ip4/{ip}/tcp/{LISTEN_PORT}"],
        node_id=dht_id(),
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
