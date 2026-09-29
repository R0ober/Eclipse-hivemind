#!/usr/bin/env python3
"""Run every config in one experiment directory.

    python scripts/run_experiment.py configs/experiment-1

Writes a manifest next to the outputs mapping each run back to the config that
produced it, because the run id is generated per launch (ADR 0001) and is the only
link between a row of results and the cell it came from.

A failing cell is recorded and the sweep continues. A sweep of seventeen cells
should not be lost because one of them timed out, and a cell that failed is itself
a result worth keeping.
"""

from __future__ import annotations

import argparse
import csv
import re
import subprocess
import sys
import time
from pathlib import Path

MANIFEST_FIELDS = ["config", "run_id", "status", "seconds", "detail"]
RUN_ID_PATTERN = re.compile(r"^RUN_ID=(\S+)$", re.M)


def read_manifest(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    with path.open(encoding="utf-8") as handle:
        return {row["config"]: row for row in csv.DictReader(handle)}


def append_manifest(path: Path, row: dict) -> None:
    is_new = not path.exists()
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=MANIFEST_FIELDS)
        if is_new:
            writer.writeheader()
        writer.writerow(row)


def run_one(config: Path, log_directory: Path) -> dict:
    command = [sys.executable, "-m", "eclipse_hivemind.cli", "--config-path", str(config)]

    started = time.monotonic()
    completed = subprocess.run(command, capture_output=True, text=True)
    seconds = round(time.monotonic() - started, 1)

    output = completed.stdout + completed.stderr
    (log_directory / f"{config.stem}.log").write_text(output, encoding="utf-8")

    match = RUN_ID_PATTERN.search(completed.stdout)
    run_id = match.group(1) if match else ""

    if completed.returncode != 0:
        detail = next(
            (line for line in reversed(output.splitlines()) if line.strip()),
            "no output",
        )
        return {"status": "failed", "run_id": run_id, "seconds": seconds, "detail": detail[:200]}
    if not run_id:
        return {"status": "no-run-id", "run_id": "", "seconds": seconds, "detail": "RUN_ID missing from output"}
    return {"status": "ok", "run_id": run_id, "seconds": seconds, "detail": ""}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("config_dir", type=Path, help="Directory of experiment configs, e.g. configs/experiment-1")
    parser.add_argument("--outputs", type=Path, default=Path("outputs"), help="Where runs are written")
    parser.add_argument("--skip-existing", action="store_true", help="Skip cells already recorded as ok")
    parser.add_argument("--dry-run", action="store_true", help="List the runs without executing them")
    args = parser.parse_args()

    configs = sorted(args.config_dir.glob("*.yaml"))
    if not configs:
        parser.error(f"no configs found in {args.config_dir}")

    manifest_path = args.outputs / f"{args.config_dir.name}-manifest.csv"
    already = read_manifest(manifest_path) if args.skip_existing else {}

    print(f"{len(configs)} configs to run")
    print(f"manifest: {manifest_path}\n")

    if args.dry_run:
        for config in configs:
            print(f"  {config.name}")
        return

    args.outputs.mkdir(parents=True, exist_ok=True)
    log_directory = args.outputs / f"{args.config_dir.name}-logs"
    log_directory.mkdir(parents=True, exist_ok=True)

    counts: dict[str, int] = {}
    for index, config in enumerate(configs, start=1):
        if str(config) in already and already[str(config)]["status"] == "ok":
            print(f"[{index}/{len(configs)}] {config.name} -> skipped")
            continue

        print(f"[{index}/{len(configs)}] {config.name} ...", end="", flush=True)
        result = run_one(config, log_directory)
        counts[result["status"]] = counts.get(result["status"], 0) + 1
        print(f" {result['status']} in {result['seconds']}s  {result['run_id']}")
        if result["detail"]:
            print(f"      {result['detail']}")
        append_manifest(manifest_path, {"config": str(config), **result})

    print("\n" + "  ".join(f"{status}={count}" for status, count in sorted(counts.items())))
    print(f"summarise with: python scripts/summarise_runs.py --manifest {manifest_path}")


if __name__ == "__main__":
    main()
