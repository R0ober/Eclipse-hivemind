#!/usr/bin/env python3
"""Check exact member identities and outcomes in the enforced-group pilot runs."""

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def verify_run(run_id: str) -> dict:
    """Reject cross-group membership and report missing or unsuccessful observations."""
    directory = ROOT / "outputs" / run_id
    config = json.loads((directory / "config.json").read_text())
    count = config["node_types"]["adversarial"]["count"]
    victim_group = {f"normal-{i}" for i in range(5 - count)} | {
        f"adversarial-{i}" for i in range(count)}
    other_group = {f"normal-{i}" for i in range(5 - count, 10 - count)}
    records = [json.loads(line) for line in (directory / "events.jsonl").read_text().splitlines()]
    identities = {}
    for record in records:
        if record["event"]["event_type"] == "node_started":
            address = record["event"]["data"]["hivemind_address"]
            identities[address.rsplit("/p2p/", 1)[1]] = record["node"]["node_id"]
    if len(identities) != 10:
        raise ValueError(f"{run_id}: expected ten startup identities")
    observed = defaultdict(set)
    evaluated = set()
    unsuccessful = mismatches = 0
    for record in records:
        event = record["event"]
        data = event["data"]
        node = record["node"]["node_id"]
        if event["event_type"] == "eval_metrics" and data["round"] == 20:
            evaluated.add(node)
        if event["event_type"] != "averaging_completed":
            continue
        index = data["round"]
        if index in observed[node]:
            raise ValueError(f"{run_id}: duplicate observation for {node}/{index}")
        observed[node].add(index)
        peers = data.get("group_members") or []
        actual = {identities.get(peer, peer) for peer in peers}
        expected = victim_group if node in victim_group else other_group
        if actual and actual != expected:
            mismatches += 1
        if data.get("success") is not True or data.get("fallback") or actual != expected:
            unsuccessful += 1
    complete = len(evaluated) == 10 and all(observed[node] == set(range(20)) for node in identities.values())
    result = dict(run_id=run_id, adversarial=count, complete=complete,
                  wrong_groups=mismatches, unsuccessful_observations=unsuccessful)
    if not complete or mismatches or unsuccessful:
        raise ValueError(f"Pilot did not meet the complete successful-membership criterion: {result}")
    return result


def main() -> None:
    """Verify the recorded successful pilot for each of the five configurations."""
    manifest = ROOT / "outputs" / "experiment-2-with-eclipse-manifest.csv"
    with manifest.open() as handle:
        rows = list(csv.DictReader(handle))
    pilots = {row["config"]: row for row in rows if row["rep"] == "1" and row["status"] == "ok"}
    if len(pilots) != 5:
        raise ValueError(f"Expected five successful pilot records, found {len(pilots)}")
    for row in pilots.values():
        print(json.dumps(verify_run(row["run_id"])), flush=True)


if __name__ == "__main__":
    main()
