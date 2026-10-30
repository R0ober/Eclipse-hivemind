#!/usr/bin/env python3
"""Measure exact averaging-partner persistence in the Experiment 2 repeats.

Run after exporting outputs/experiment-2-manifest.csv with summarise_runs.py.
Uses raw completion events for success, and resolved membership.csv for identities.
"""

from pathlib import Path
import json

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "analysis" / "experiment-3" / "experiment-2"
DESTINATION = DATA.parent


def load_verified_observations(runs: pd.DataFrame) -> pd.DataFrame:
    """Join member sets to their original success flags without collapsing duplicates."""
    records = []
    for run_id in runs.run_id:
        path = ROOT / "outputs" / run_id / "events.jsonl"
        with path.open() as handle:
            for line in handle:
                record = json.loads(line)
                event = record["event"]
                if event["event_type"] == "averaging_completed" and record["node"]["node_type"] == "normal":
                    records.append({
                        "run_id": run_id, "node_id": record["node"]["node_id"],
                        "round": event["data"]["round"],
                        "success": event["data"].get("success") is True,
                    })
    keys = ["run_id", "node_id", "round"]
    completion = pd.DataFrame(records)
    if completion.duplicated(keys).any():
        raise ValueError("Duplicate raw averaging-completion keys")
    members = pd.read_csv(DATA / "membership.csv")
    members = members[members.node_type.eq("normal")]
    if members.duplicated(keys).any():
        raise ValueError("Duplicate membership keys")
    observations = completion.merge(members, on=keys, how="left", validate="one_to_one")
    observations["member_set"] = observations.group_members.fillna("").map(
        lambda value: "|".join(sorted(value.split("|"))) if value else "")
    observations["valid"] = (
        observations.success & observations.unknown_in_group.eq(0)
        & observations.group_size.eq(5) & observations.member_set.ne("")
        & (observations.honest_in_group + observations.adversary_in_group).eq(5)
    )
    return observations


def summarise_node_history(history: pd.DataFrame) -> dict:
    """Count unchanged groups, adjacent retention and longest contiguous sequences."""
    history = history.sort_values("round")
    usable = history[history.valid]
    fixed = (len(history) == 20 and set(history["round"]) == set(range(20))
             and history.valid.all() and usable.member_set.nunique() == 1)
    retained = eligible = longest = streak = 0
    previous_round = previous_members = None
    previous_valid = False
    for row in history.itertuples():
        adjacent = previous_valid and row.valid and row.round == previous_round + 1
        if adjacent:
            eligible += 1
            retained += int(row.member_set == previous_members)
        if row.valid:
            streak = streak + 1 if adjacent and row.member_set == previous_members else 1
            longest = max(longest, streak)
        else:
            streak = 0
        previous_round, previous_members, previous_valid = row.round, row.member_set, row.valid
    return {
        "completion_records": len(history), "valid_observations": len(usable),
        "invalid_observations": int((~history.valid).sum()),
        "missing_expected_indices": len(set(range(20)) - set(history["round"])),
        "distinct_valid_groups": usable.member_set.nunique(),
        "unchanged_all_20": bool(fixed), "retained_adjacent_pairs": retained,
        "eligible_adjacent_pairs": eligible, "longest_unchanged_sequence": longest,
    }


def main() -> None:
    """Export node and run summaries, preserving the run as the comparison unit."""
    runs = pd.read_csv(DATA / "runs.csv")
    if not runs.run_id.is_unique:
        raise ValueError("Duplicate run IDs")
    observations = load_verified_observations(runs)
    rows = []
    for run in runs.itertuples():
        for index in range(int(run.honest)):
            node = f"normal-{index}"
            history = observations[observations.run_id.eq(run.run_id) & observations.node_id.eq(node)]
            rows.append({"run_id": run.run_id, "adversarial": run.adversarial,
                         "node_id": node, **summarise_node_history(history)})
    nodes = pd.DataFrame(rows)
    per_run = nodes.groupby(["run_id", "adversarial"]).agg(
        honest_nodes=("node_id", "size"), all_honest_unchanged_20=("unchanged_all_20", "all"),
        any_honest_unchanged_20=("unchanged_all_20", "any"),
        fixed_nodes=("unchanged_all_20", "sum"),
        retained_pairs=("retained_adjacent_pairs", "sum"),
        eligible_pairs=("eligible_adjacent_pairs", "sum"),
        mean_longest_sequence=("longest_unchanged_sequence", "mean"),
        invalid_observations=("invalid_observations", "sum"),
        missing_expected_indices=("missing_expected_indices", "sum"),
    ).reset_index()
    per_run["adjacent_retention"] = per_run.retained_pairs / per_run.eligible_pairs.replace(0, float("nan"))
    summary = per_run.groupby("adversarial").agg(
        runs=("run_id", "size"), all_honest_fixed_runs=("all_honest_unchanged_20", "sum"),
        any_honest_fixed_runs=("any_honest_unchanged_20", "sum"),
        median_run_retention=("adjacent_retention", "median"),
        median_mean_longest_sequence=("mean_longest_sequence", "median"),
        invalid_observations=("invalid_observations", "sum"),
        missing_expected_indices=("missing_expected_indices", "sum"),
    )
    nodes.to_csv(DESTINATION / "group-persistence-nodes.csv", index=False)
    per_run.to_csv(DESTINATION / "group-persistence-runs.csv", index=False)
    summary.to_csv(DESTINATION / "group-persistence-summary.csv")
    # Keep a predesignated victim outcome for the later manipulated-membership comparison.
    rounds = pd.read_csv(DATA / "rounds.csv")
    victim = rounds[rounds.node_id.eq("normal-0") & rounds["round"].eq(20)].merge(
        runs[["run_id", "adversarial"]], on="run_id", validate="one_to_one")
    victim.to_csv(DESTINATION / "designated-victim-final.csv", index=False)
    print(summary.to_string())


if __name__ == "__main__":
    main()
