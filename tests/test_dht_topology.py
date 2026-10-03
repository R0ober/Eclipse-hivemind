"""Check topology capture, identity resolution and unresolved neighbourhoods."""

import asyncio
import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location("summarise_runs", ROOT / "scripts/summarise_runs.py")
summary = importlib.util.module_from_spec(spec)
spec.loader.exec_module(summary)


def record(kind, node_id, node_type, data):
    return {"node": {"node_id": node_id, "node_type": node_type},
            "event": {"event_type": kind, "data": data}}


def test_topology_resolves_later_startups_and_separates_observers():
    events = [record("dht_snapshot", "observer-0", "observer", {
        "tick": 0, "tick_kind": "observation", "dht_id": "0" * 40,
        "known_peers": [("1" * 40, "honest-peer"), ("2" * 40, "bad-peer")],
        "target_dht_id": "3" * 40,
        "nearest_peers": [("2" * 40, "bad-peer"), ("0" * 40, "observer-peer")],
    })]
    for node_id, node_type, peer_id in [("normal-0", "normal", "honest-peer"),
                                        ("adversarial-0", "adversarial", "bad-peer"),
                                        ("observer-0", "observer", "observer-peer")]:
        events.append(record("node_started", node_id, node_type, {"hivemind_address": f"/p2p/{peer_id}"}))
    routing, nearest = summary.summarise_topology("run", events, {})
    assert routing["neighbourhood_members"] == "normal-0|adversarial-0"
    assert routing["tick_kind"] == "observation"
    assert routing["adversarial_share_of_neighbourhood"] == 0.5
    assert nearest["observer_peers"] == 1
    assert nearest["adversarial_peers"] == 1
    assert nearest["adversarial_share_of_neighbourhood"] == 0.5


def test_unknown_and_empty_neighbourhoods_have_no_share():
    for peers in [[], [("1" * 40, "full-unknown-peer-id")]]:
        events = [record("dht_snapshot", "observer-0", "observer",
                         {"tick": 0, "tick_kind": "observation", "dht_id": "0" * 40,
                          "known_peers": peers})]
        row = summary.summarise_topology("run", events, {})[0]
        assert row["adversarial_share_of_neighbourhood"] == ""
        assert row["adversarial_peers"] == 0
        assert row["unknown_peers"] == len(peers)


def test_snapshot_copies_routing_table_before_lookup():
    pytest.importorskip("hivemind")
    sys.path.insert(0, str(ROOT / "src/eclipse_hivemind/nodes"))
    from shared.dht import routing_snapshot
    from hivemind.dht.routing import DHTID

    bucket = SimpleNamespace(nodes_to_peer_id={DHTID(2): "known-peer"})

    async def find_nearest_nodes(queries, k_nearest):
        assert k_nearest == 2
        bucket.nodes_to_peer_id[DHTID(3)] = "discovered-peer"
        return {queries[0]: {DHTID(1): "self-peer", DHTID(3): "discovered-peer"}}

    node = SimpleNamespace(node_id=DHTID(1),
                           protocol=SimpleNamespace(routing_table=SimpleNamespace(buckets=[bucket])),
                           find_nearest_nodes=find_nearest_nodes)
    dht = SimpleNamespace(run_coroutine=lambda callback: asyncio.run(callback(None, node)))
    snapshot = routing_snapshot(dht, "0" * 39 + "1", 2)
    assert snapshot["known_peers"] == [("0" * 39 + "2", "known-peer")]
    assert len(snapshot["nearest_peers"]) == 2
    assert snapshot["dht_id"] == "0" * 39 + "1"



def test_report_dht_snapshot_accepts_experiment_defined_tick_kind(monkeypatch):
    pytest.importorskip("hivemind")
    sys.path.insert(0, str(ROOT / "src/eclipse_hivemind/nodes"))
    from shared import topology

    snapshot = {"dht_id": "0" * 40, "known_peers": []}
    monkeypatch.setattr(topology, "routing_snapshot", lambda *args: snapshot)
    reports = []
    monkeypatch.setattr(topology.client, "send_dht_snapshot", lambda **kwargs: reports.append(kwargs))
    assert topology.report_dht_snapshot(
        None, aggregator_endpoint="http://aggregator", identity={},
        tick=0, tick_kind="attack_stage",
    ) == snapshot
    assert reports[0]["tick_kind"] == "attack_stage"
