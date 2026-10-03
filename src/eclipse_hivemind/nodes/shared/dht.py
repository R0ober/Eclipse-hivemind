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


def _serialise_peers(nodes_to_peer_id: dict) -> list[tuple[str, str]]:
    """Keep both routing IDs and network identities, in stable ID order."""
    return sorted((f"{int(node_id):040x}", str(peer_id)) for node_id, peer_id in nodes_to_peer_id.items())


async def _read_routing_snapshot(_dht: hivemind.DHT, node, target_id: str | None, k_nearest: int) -> dict:
    """Read live routing state inside the DHT subprocess (hivemind 1.1.12).

    Copy the table before querying: a nearest-node lookup can itself discover peers.
    This callback must stay importable so run_coroutine can ship it to the child.
    """
    known_peers = {}
    for bucket in node.protocol.routing_table.buckets:
        known_peers.update(bucket.nodes_to_peer_id)
    snapshot = {"dht_id": f"{int(node.node_id):040x}", "known_peers": _serialise_peers(known_peers)}
    if target_id is not None:
        target = DHTID(int(target_id, 16))
        nearest = await node.find_nearest_nodes([target], k_nearest=k_nearest)
        snapshot.update(target_dht_id=target_id, k_nearest=k_nearest,
                        nearest_peers=_serialise_peers(nearest[target]))
    return snapshot


def routing_snapshot(dht: hivemind.DHT, target_dht_id: str | None = None, k_nearest: int = 20) -> dict:
    """Return this node's routing table and optionally the nearest peers to a target.

    The nearest query includes this node when it belongs among the k closest peers.
    Re-verify the bucket layout when upgrading from hivemind 1.1.12.
    """
    import functools

    if k_nearest <= 0:
        raise ValueError("k_nearest must be greater than zero")
    if target_dht_id is not None:
        if len(target_dht_id) != 40 or any(char not in "0123456789abcdefABCDEF" for char in target_dht_id):
            raise ValueError("target_dht_id must be a 40-character hexadecimal DHTID")
        target_dht_id = target_dht_id.lower()
    return dht.run_coroutine(functools.partial(
        _read_routing_snapshot, target_id=target_dht_id, k_nearest=k_nearest,
    ))
