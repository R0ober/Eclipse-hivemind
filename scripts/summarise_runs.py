#!/usr/bin/env python3
"""Turn run event logs into two tables you can plot or paste into a sheet.

    python scripts/summarise_runs.py                                  # every run in outputs/
    python scripts/summarise_runs.py --manifest outputs/experiment-1-manifest.csv
    python scripts/summarise_runs.py --csv results/

`runs.csv` is one row per run: the final state of each cell.
`rounds.csv` is one row per run, round and node type: the curves.

Both split honest from adversarial nodes. With strict averaging groups the peers
are no longer all in the same group, so they no longer hold identical weights, and
a mean over every node in the swarm would average the attacker's model into the
result it is supposed to be damaging.

Read `averaging_success_rate` before reading any loss. A cell where averaging
stopped succeeding is measuring a broken swarm, not a successful attack, and the
two look alike in a loss curve.
"""

from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import defaultdict
from pathlib import Path

RUN_FIELDS = [
    "run_id", "config", "experiment_name",
    "peers", "honest", "adversarial",
    "target_batch_size", "target_group_size", "min_group_size",
    "final_round",
    "honest_eval_loss", "honest_eval_loss_sd",
    "honest_eval_accuracy", "honest_eval_accuracy_sd",
    "honest_max_class_fraction",
    "adversarial_eval_loss", "adversarial_eval_accuracy",
    "averaging_success_rate", "honest_averaging_success_rate",
    "mean_group_size", "group_sizes", "top_fallback_reason",
    "total_samples", "honest_samples", "min_peer_samples", "max_peer_samples",
    "nodes_started", "distinct_fingerprints", "node_errors",
]

ROUND_FIELDS = [
    "run_id", "config", "round", "node_type", "nodes",
    "eval_loss", "eval_accuracy", "max_class_fraction",
]


def mean(values: list[float]) -> float | str:
    return round(statistics.mean(values), 6) if values else ""


def spread(values: list[float]) -> float | str:
    return round(statistics.stdev(values), 6) if len(values) > 1 else 0.0 if values else ""


def load_events(path: Path) -> list[dict]:
    events = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def summarise(run_id: str, events: list[dict], meta: dict) -> tuple[dict, list[dict]]:
    node_type_of: dict[str, str] = {}
    settings: dict = {}
    fingerprints: set[str] = set()
    experiment_name = ""
    started: set[str] = set()
    errors = 0

    # round -> node_type -> list of (loss, accuracy, max_class_fraction)
    evals: dict[int, dict[str, list[tuple]]] = defaultdict(lambda: defaultdict(list))
    samples: dict[str, int] = defaultdict(int)
    averaging: dict[str, list[dict]] = defaultdict(list)

    for record in events:
        event, node = record["event"], record["node"]
        node_id, node_type = node["node_id"], node["node_type"]
        node_type_of[node_id] = node_type
        kind, data = event["event_type"], event["data"]
        experiment_name = experiment_name or record.get("experiment_id", "")

        if kind == "node_started":
            started.add(node_id)
            settings = settings or data.get("settings", {})
            if data.get("model_fingerprint"):
                fingerprints.add(data["model_fingerprint"])
        elif kind == "training_metrics":
            samples[node_id] += data["samples"]
        elif kind == "eval_metrics":
            fractions = data.get("predicted_class_fractions") or []
            evals[data["round"]][node_type].append(
                (data["eval_loss"], data["eval_accuracy"], max(fractions) if fractions else None)
            )
        elif kind == "averaging_completed":
            averaging[node_type].append(data)
        elif kind == "node_error":
            errors += 1

    counts: dict[str, int] = defaultdict(int)
    for node_type in node_type_of.values():
        counts[node_type] += 1

    rounds = []
    for round_number in sorted(evals):
        for node_type, measurements in sorted(evals[round_number].items()):
            fractions = [f for _, _, f in measurements if f is not None]
            rounds.append({
                "run_id": run_id,
                "config": meta.get("config", ""),
                "round": round_number,
                "node_type": node_type,
                "nodes": len(measurements),
                "eval_loss": mean([loss for loss, _, _ in measurements]),
                "eval_accuracy": mean([accuracy for _, accuracy, _ in measurements]),
                "max_class_fraction": mean(fractions),
            })

    final_round = max(evals) if evals else ""
    final = evals.get(final_round, {}) if evals != {} else {}

    def final_of(node_type: str, index: int) -> list[float]:
        return [row[index] for row in final.get(node_type, []) if row[index] is not None]

    all_averaging = [row for rows in averaging.values() for row in rows]
    honest_averaging = averaging.get("normal", [])
    group_sizes = [row["group_size"] for row in all_averaging if row.get("group_size")]
    fallbacks: dict[str, int] = defaultdict(int)
    for row in all_averaging:
        if row.get("fallback") and row.get("fallback_reason"):
            fallbacks[str(row["fallback_reason"])[:60]] += 1

    honest_samples = sum(count for node, count in samples.items() if node_type_of.get(node) == "normal")

    run = {
        "run_id": run_id,
        "config": meta.get("config", ""),
        "experiment_name": experiment_name,
        "peers": len(node_type_of),
        "honest": counts.get("normal", 0),
        "adversarial": sum(count for name, count in counts.items() if name != "normal"),
        "target_batch_size": settings.get("target_batch_size", ""),
        "target_group_size": settings.get("target_group_size", ""),
        "min_group_size": settings.get("min_group_size", ""),
        "final_round": final_round,
        "honest_eval_loss": mean(final_of("normal", 0)),
        "honest_eval_loss_sd": spread(final_of("normal", 0)),
        "honest_eval_accuracy": mean(final_of("normal", 1)),
        "honest_eval_accuracy_sd": spread(final_of("normal", 1)),
        "honest_max_class_fraction": mean(final_of("normal", 2)),
        "adversarial_eval_loss": mean(final_of("adversarial", 0)),
        "adversarial_eval_accuracy": mean(final_of("adversarial", 1)),
        "averaging_success_rate": mean([1.0 if row["success"] else 0.0 for row in all_averaging]),
        "honest_averaging_success_rate": mean([1.0 if row["success"] else 0.0 for row in honest_averaging]),
        "mean_group_size": mean(group_sizes),
        "group_sizes": "|".join(str(size) for size in sorted(set(group_sizes))),
        "top_fallback_reason": max(fallbacks, key=fallbacks.get) if fallbacks else "",
        "total_samples": sum(samples.values()),
        "honest_samples": honest_samples,
        "min_peer_samples": min(samples.values()) if samples else "",
        "max_peer_samples": max(samples.values()) if samples else "",
        "nodes_started": len(started),
        "distinct_fingerprints": len(fingerprints),
        "node_errors": errors,
    }
    return run, rounds


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--outputs", type=Path, default=Path("outputs"))
    parser.add_argument("--manifest", type=Path, default=None, help="Manifest from run_experiment.py, to label runs with their config and seed")
    parser.add_argument("--csv", type=Path, default=None, help="Directory to write runs.csv and rounds.csv into")
    args = parser.parse_args()

    meta_by_run: dict[str, dict] = {}
    selected: list[str] | None = None
    if args.manifest and args.manifest.exists():
        with args.manifest.open(encoding="utf-8") as handle:
            rows = [row for row in csv.DictReader(handle) if row.get("run_id")]
        meta_by_run = {row["run_id"]: {"config": Path(row["config"]).name} for row in rows}
        selected = [row["run_id"] for row in rows if row["status"] == "ok"]

    run_rows, round_rows = [], []
    for directory in sorted(args.outputs.glob("*/")):
        events_path = directory / "events.jsonl"
        if not events_path.exists():
            continue
        run_id = directory.name
        if selected is not None and run_id not in selected:
            continue
        events = load_events(events_path)
        if not events:
            continue
        run, rounds = summarise(run_id, events, meta_by_run.get(run_id, {}))
        run_rows.append(run)
        round_rows.extend(rounds)

    if not run_rows:
        print(f"no runs with events found under {args.outputs}")
        return

    columns = ["run_id", "config", "peers", "honest", "adversarial", "final_round",
               "honest_eval_loss", "honest_eval_accuracy", "honest_max_class_fraction",
               "averaging_success_rate", "mean_group_size", "nodes_started"]
    widths = {name: max(len(name), *(len(str(row[name])) for row in run_rows)) for name in columns}
    print("  ".join(name.ljust(widths[name]) for name in columns))
    for row in sorted(run_rows, key=lambda r: str(r["config"])):
        print("  ".join(str(row[name]).ljust(widths[name]) for name in columns))

    for row in run_rows:
        if row["distinct_fingerprints"] > 1:
            print(f"\n  warning: {row['run_id']} has {row['distinct_fingerprints']} distinct model fingerprints "
                  f"- peers did not start from the same weights")
        if row["nodes_started"] != row["peers"]:
            print(f"\n  warning: {row['run_id']} started {row['nodes_started']} of {row['peers']} nodes")

    if args.csv:
        args.csv.mkdir(parents=True, exist_ok=True)
        for name, fields, rows in [("runs.csv", RUN_FIELDS, run_rows), ("rounds.csv", ROUND_FIELDS, round_rows)]:
            with (args.csv / name).open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fields)
                writer.writeheader()
                writer.writerows(rows)
            print(f"\nwrote {args.csv / name} ({len(rows)} rows)")


if __name__ == "__main__":
    main()
