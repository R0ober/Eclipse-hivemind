#!/usr/bin/env python3
"""Run every config in one experiment directory, optionally several times each.

    python scripts/run_experiment.py configs/experiment-1
    python scripts/run_experiment.py configs/experiment-2 --repeat 100

Each run appends a row to a manifest next to the outputs, mapping the config and
repeat number to the run id it produced. The run id is generated per launch
(ADR 0001), so the manifest is the only link between a result row and the cell that
produced it - keep it with the outputs.

`--repeat` is how the reproducibility study runs: the seed is fixed, so repeats
differ only in container scheduling, which is exactly the run-to-run variation we
want to measure. Every repeat is its own run with its own run id, so the summariser
sees N independent rows per cell to take a mean and spread over.

A failing cell is recorded and the sweep continues. A long sweep should not be lost
because one run timed out, and a failed run is itself a result worth keeping.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time
from pathlib import Path

MANIFEST_FIELDS = ["config", "rep", "run_id", "status", "seconds", "detail"]
RUN_ID_PATTERN = re.compile(r"^RUN_ID=(\S+)$", re.M)


def read_manifest(path: Path) -> dict[tuple[str, str], dict]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        return {(row["config"], row["rep"]): row for row in csv.DictReader(handle)}


def append_manifest(path: Path, row: dict) -> None:
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def run_one(config: Path, rep: int, repeats: int, log_directory: Path, outputs: Path = Path("outputs")) -> dict:
    command = [sys.executable, "-m", "eclipse_hivemind.cli", "--config-path", str(config), "--outputs", str(outputs)]

    started = time.monotonic()
    completed = subprocess.run(command, capture_output=True, text=True)
    seconds = round(time.monotonic() - started, 1)

    output = completed.stdout + completed.stderr
    log_name = f"{config.stem}.log" if repeats == 1 else f"{config.stem}-rep{rep:03d}.log"
    (log_directory / log_name).write_text(output, encoding="utf-8")

    match = RUN_ID_PATTERN.search(completed.stdout)
    run_id = match.group(1) if match else ""

    if completed.returncode != 0:
        detail = next((line for line in reversed(output.splitlines()) if line.strip()), "no output")
        return {"status": "failed", "run_id": run_id, "seconds": seconds, "detail": detail[:200]}
    if not run_id:
        return {"status": "no-run-id", "run_id": "", "seconds": seconds, "detail": "RUN_ID missing from output"}
    return {"status": "ok", "run_id": run_id, "seconds": seconds, "detail": ""}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config_dir", type=Path, help="Directory of experiment configs, e.g. configs/experiment-1")
    parser.add_argument("--repeat", type=int, default=1, help="Run each config this many times (default 1)")
    parser.add_argument("--outputs", type=Path, default=Path("outputs"), help="Where runs are written")
    parser.add_argument("--skip-existing", action="store_true", help="Skip config/rep pairs already recorded as ok")
    parser.add_argument("--dry-run", action="store_true", help="List the runs without executing them")
    args = parser.parse_args()

    if args.repeat < 1:
        parser.error("--repeat must be at least 1")

    configs = sorted(args.config_dir.glob("*.yaml"))
    if not configs:
        parser.error(f"no configs found in {args.config_dir}")

    manifest_path = args.outputs / f"{args.config_dir.name}-manifest.csv"
    already = read_manifest(manifest_path) if args.skip_existing else {}

    planned = [(config, rep) for rep in range(1, args.repeat + 1) for config in configs]
    print(f"{len(configs)} configs x {args.repeat} repeats = {len(planned)} runs")
    print(f"manifest: {manifest_path}\n")

    if args.dry_run:
        for config, rep in planned:
            print(f"  {config.name}  rep {rep}")
        return

    args.outputs.mkdir(parents=True, exist_ok=True)
    log_directory = args.outputs / f"{args.config_dir.name}-logs"
    log_directory.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    for index, (config, rep) in enumerate(planned, start=1):
        key = (str(config), str(rep))
        if key in already and already[key]["status"] == "ok":
            print(f"[{index}/{len(planned)}] {config.name} rep {rep} -> skipped")
            continue

        print(f"[{index}/{len(planned)}] {config.name} rep {rep} ...", end="", flush=True)
        result = run_one(config, rep, args.repeat, log_directory, args.outputs)
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        print(f" {result['status']} in {result['seconds']}s  {result['run_id']}")
        if result["detail"]:
            print(f"      {result['detail']}")
        append_manifest(manifest_path, {"config": str(config), "rep": rep, **result})

    print("\n" + "  ".join(f"{status}={count}" for status, count in sorted(counts.items())))
    print(f"summarise with: python scripts/summarise_runs.py --manifest {manifest_path}")


if __name__ == "__main__":
    main()
