"""Derive holder shares from observed replica storage, with coverage reported."""

from __future__ import annotations


def holder_metrics(snapshots: dict, expected_nodes: list[str], now: float, max_age: float) -> dict:
    missing = [node for node in expected_nodes if node not in snapshots or
               now - snapshots[node]["dht_time"] > max_age]
    observed = {node: sample for node, sample in snapshots.items() if node in expected_nodes and node not in missing}
    holders = {}
    for node, sample in observed.items():
        for record in sample["replica_records"]:
            if record["expiration_time"] <= now or "peer_id" not in record:
                continue
            # An announcement version is the subkey, active flag and expiration.
            key = (record["peer_id"], record["active"], record["expiration_time"])
            holders.setdefault(key, {})[node] = sample["storage_role"]
    return {"sampling_time": now, "max_snapshot_age_seconds": max_age,
            "expected_nodes": expected_nodes, "observed_nodes": list(observed), "missing_or_stale_nodes": missing,
            "complete_coverage": not missing and bool(expected_nodes),
            "records": [{"peer_id": peer, "active": active, "expiration_time": expiration,
                         "holder_nodes": list(nodes), "identified_holders": len(nodes),
                         "malicious_holders": sum(role == "malicious" for role in nodes.values()),
                         "storage_control_share": sum(role == "malicious" for role in nodes.values()) / len(nodes)
                                                  if not missing else None}
                        for (peer, active, expiration), nodes in holders.items()]}
