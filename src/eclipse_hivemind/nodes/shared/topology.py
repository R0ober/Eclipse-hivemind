"""Capture and report DHT topology for any node type."""

from __future__ import annotations

import hivemind

from . import client
from .dht import routing_snapshot

# What a reported tick counts. A node type picks the one its schedule actually
# follows; analysis needs this to know which snapshots share a clock.
TICK_OBSERVATION = "observation"
TICK_LOCAL_EPOCH = "local_epoch"


def report_dht_snapshot(
    dht: hivemind.DHT,
    *,
    aggregator_endpoint: str,
    identity: dict,
    tick: int,
    tick_kind: str,
    target_dht_id: str | None = None,
    k_nearest: int = 20,
) -> dict:
    """Capture this node's current DHT view and send it to the aggregator.

    Include the routing-table peers and, when a target DHTID is supplied, the
    nearest peers to that target. Return the reported snapshot so the caller
    can also describe it in local logs.

    The caller chooses when to report, what its tick counts, and says so with
    tick_kind: an observer counts observations on a timer, while a training node
    counts local optimizer epochs. There is no default, because a snapshot filed under
    the wrong clock cannot be lined up with anything else in the run.
    Capture or delivery errors propagate to the caller.
    """
    snapshot = routing_snapshot(dht, target_dht_id, k_nearest)
    client.send_dht_snapshot(
        aggregator_endpoint=aggregator_endpoint,
        **identity,
        tick=tick,
        tick_kind=tick_kind,
        snapshot=snapshot,
    )
    return snapshot
