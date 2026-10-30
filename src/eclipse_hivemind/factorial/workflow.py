from __future__ import annotations

import argparse
import csv
from collections import Counter, deque
from contextlib import contextmanager
import fcntl
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import subprocess
import sys
import tarfile
import threading
import time

from . import study, failures
from .evidence import analyse_path, expected_nodes

FIELDS = ["phase", "cell", "seed", "run_id", "config", "config_sha256", "status", "seconds", "raw_bytes",
          "archive", "archive_sha256", "metadata", "detail"]
LOCAL_RESERVE_BYTES = 8_000_000_000
ARCHIVE_RESERVE_BYTES = 20_000_000_000


def run_logged(command, log_path, env, heartbeat_seconds=30):
    """Retain full child output and make long running trials visible to the user."""
    stopped = threading.Event()
    began = time.monotonic()

    def heartbeat():
        while not stopped.wait(heartbeat_seconds):
            print(f"  Still running ({time.monotonic() - began:.0f}s); log: {log_path}", flush=True)

    thread = threading.Thread(target=heartbeat, daemon=True)
    print(f"  Live launcher log: {log_path}", flush=True)
    thread.start()
    try:
        with log_path.open("x") as log:
            subprocess.run(command, cwd=study.ROOT, env=env, stdout=log, stderr=subprocess.STDOUT, check=True)
    finally:
        stopped.set()
        thread.join()


@contextmanager
def campaign_lock(root):
    root.mkdir(parents=True, exist_ok=True)
    with (root / ".campaign.lock").open("a") as stream:
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("this campaign already has an active owner") from error
        try:
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)


def manifest_rows(root):
    path = root / "manifest.csv"
    if not path.exists():
        return []
    with path.open() as stream:
        return list(csv.DictReader(stream))


def append(root, row):
    path = root / "manifest.csv"
    exists = path.exists()
    with path.open("a", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=FIELDS)
        if not exists:
            writer.writeheader()
        writer.writerow({key: row.get(key, "") for key in FIELDS})
        stream.flush()
        os.fsync(stream.fileno())


def verify_archive(path, metadata):
    if study.sha(path) != metadata["archive_sha256"]:
        raise ValueError("archive checksum mismatch")
    found = {}
    with tarfile.open(path, "r:gz") as archive:
        for member in archive:
            if not member.isfile() or member.name not in metadata["members"] or member.name in found:
                raise ValueError("unexpected or duplicate archive member")
            with archive.extractfile(member) as stream:
                found[member.name] = hashlib.file_digest(stream, "sha256").hexdigest()
    if found != metadata["members"]:
        raise ValueError("archive contents do not match source hashes")


def raw_inventory(directory):
    files = sorted(Path(directory).rglob("*"))
    if any(path.is_symlink() or (not path.is_file() and not path.is_dir()) for path in files):
        raise ValueError("non-regular raw artifacts are not supported")
    return {str(path.relative_to(directory)): study.sha(path) for path in files if path.is_file()}


def validate_container_records(records, run_id, images):
    expected = {f"{run_id}-{node}": images[study.IMAGE] for node in expected_nodes()}
    expected[f"{run_id}-logger"] = images[study.IMAGES["logger"]]
    expected[f"{run_id}-attack-coordinator"] = images[study.IMAGES["coordinator"]]
    seen = set()
    for artifact in records:
        name, state = artifact["name"], artifact["state"]
        if name not in expected or name in seen:
            raise ValueError(f"unexpected or duplicate container artifact: {name}")
        seen.add(name)
        if state.get("Running") or state.get("OOMKilled") or state.get("ExitCode") != 0:
            raise ValueError(f"non-clean container exit: {name}")
        if artifact["image_id"] != expected[name]:
            raise ValueError(f"actual container image differed from the lock: {name}")
    if seen != expected.keys():
        raise ValueError("missing node/logger/coordinator exit-state artifacts")


def check_storage(staging, archive_parent):
    for path, reserve, label in ((staging, LOCAL_RESERVE_BYTES, "local SSD"),
                                 (archive_parent, ARCHIVE_RESERVE_BYTES, "archive volume")):
        free = shutil.disk_usage(path).free
        if free < reserve:
            raise ValueError(f"insufficient {label} reserve: {free / 1e9:.2f} GB free, "
                             f"{reserve / 1e9:.0f} GB required; stopped before next run")


def archive_run(directory, archive_root, trial, evidence, *, exclusion=None):
    """Preserve every closed raw file, including SQLite; no old data is removed."""
    directory, archive_root = Path(directory), Path(archive_root)
    members = raw_inventory(directory)
    archive_root.mkdir(parents=True, exist_ok=True)
    target = archive_root / f"{trial['run_id']}.tar.gz"
    sidecar = target.with_suffix(".json")
    if target.exists() or sidecar.exists():
        raise ValueError("archive identity already exists; refusing overwrite")
    partial = target.with_suffix(".partial")
    if partial.exists():
        raise ValueError("incomplete archive exists; inspect before continuing")
    with tarfile.open(partial, "w:gz", compresslevel=1) as archive:
        for name in members:
            archive.add(directory / name, arcname=name, recursive=False)
    with partial.open("rb") as stream:
        os.fsync(stream.fileno())
    metadata = dict(trial=trial, evidence=evidence, members=members, archive_sha256=study.sha(partial),
                    raw_bytes=sum((directory / name).stat().st_size for name in members),
                    archive_bytes=partial.stat().st_size)
    if exclusion is not None:
        metadata["exclusion"] = exclusion
    verify_archive(partial, metadata)
    # Detect writes during compression; logger must really have been stopped.
    if members != raw_inventory(directory):
        raise ValueError("raw artifacts changed during archival")
    partial.rename(target)
    study.immutable(sidecar, metadata)
    return target, sidecar, metadata


def image_lock():
    path = study.RESULTS / "image-lock.json"
    if not path.exists():
        raise ValueError("missing image lock; run the preparation command first")
    value = json.loads(path.read_text())
    if value["sources"] != study.sources() or value.get("probe", {}).get("complete") is not True:
        raise ValueError("source/image probe lock changed; do not mix software versions")
    return value


def verify_images(value):
    for tag, expected in value["images"].items():
        inspected = subprocess.run(["docker", "image", "inspect", tag], capture_output=True, text=True)
        if inspected.returncode:
            # Recover only the known alias inconsistency, using the same locked ID.
            direct = json.loads(subprocess.check_output(["docker", "image", "inspect", expected], text=True))[0]
            if direct["Id"] != expected:
                raise ValueError("locked image is unavailable")
            subprocess.run(["docker", "tag", expected, tag], check=True)
            inspected = subprocess.run(["docker", "image", "inspect", tag], check=True, capture_output=True, text=True)
        if json.loads(inspected.stdout)[0]["Id"] != expected:
            raise ValueError(f"image no longer matches lock: {tag}")


def verify_completed(row):
    metadata = json.loads(Path(row["metadata"]).read_text())
    if any(str(metadata["trial"].get(key)) != str(row.get(key)) for key in ("run_id", "cell", "seed", "config_sha256", "phase")) or metadata["archive_sha256"] != row["archive_sha256"]:
        raise ValueError("manifest/archive identity mismatch")
    if metadata["evidence"].get("technical_valid") is not True:
        raise ValueError("archived measurement is not technically valid")
    verify_archive(Path(row["archive"]), metadata)
    return metadata


def verify_trial_inputs(inputs, protocol, row):
    frozen = protocol["image_lock"]
    if (inputs["sources"] != frozen["sources"] or inputs["images"] != frozen["images"]
            or inputs["config_sha256"] != row["config_sha256"]):
        raise ValueError("source, image or configuration lock differs within campaign")


def freeze_source_bundle(archive_root):
    members = study.sources()
    local = study.RESULTS / "source-snapshot.tar.gz"
    sidecar = study.RESULTS / "source-snapshot.json"
    if not local.exists():
        with tarfile.open(local, "w:gz", compresslevel=1) as archive:
            for name in members:
                archive.add(study.ROOT / name, arcname=name, recursive=False)
        study.immutable(sidecar, dict(members=members, archive_sha256=study.sha(local)))
    metadata = json.loads(sidecar.read_text())
    if metadata["members"] != members:
        raise ValueError("frozen source snapshot no longer matches")
    verify_archive(local, metadata)
    destination = archive_root.parent / local.name
    if not destination.exists():
        partial = destination.with_suffix(".partial")
        with partial.open("xb") as stream, local.open("rb") as source:
            shutil.copyfileobj(source, stream)
            stream.flush()
            os.fsync(stream.fileno())
        if study.sha(partial) != metadata["archive_sha256"]:
            raise ValueError("source snapshot copy did not verify")
        partial.rename(destination)
    if study.sha(destination) != metadata["archive_sha256"]:
        raise ValueError("archived source snapshot changed")
    study.immutable(archive_root.parent / sidecar.name, metadata)


def pilot_forecast(rows):
    pilots = [row for row in rows if row["phase"] == "pilot" and row["status"] == "ok"]
    metadata = [json.loads(Path(row["metadata"]).read_text()) for row in pilots]
    complete = len(pilots) == 8 and {row["cell"] for row in pilots} == set(study.CELLS)
    passed = complete and all(item["evidence"]["technical_valid"] and item["evidence"]["endpoint_complete"] for item in metadata)
    return dict(passed=passed, pilot_trials=len(pilots), rounds=study.ROUNDS,
                projected_main_archive_bytes=10 * sum(item["archive_bytes"] for item in metadata) if complete else None,
                required_archive_bytes_with_allowance=15 * sum(item["archive_bytes"] for item in metadata) + 20_000_000_000 if complete else None,
                projected_main_hours_including_archive=10 * sum(float(row["seconds"]) for row in pilots) / 3600 if complete else None,
                note="One pilot per cell; forecast is not an upper bound. Attack outcomes do not select primary cells.")


def run(phase, archive_root, resume=False):
    plan = study.plan(phase)
    planned_trials = {(row["cell"], row["seed"]): row for row in plan}
    total = len(plan)
    print(f"Checking {study.VERSION}: {phase}, {total} trials, {study.ROUNDS} rounds.", flush=True)
    study.prepare_configs()
    frozen = image_lock()
    verify_images(frozen)
    if archive_root.is_relative_to(study.RESULTS):
        raise ValueError("raw archives need a separate explicit destination")
    archive_root.parent.mkdir(parents=True, exist_ok=True)
    archive_device = archive_root.parent.stat().st_dev
    with campaign_lock(study.RESULTS):
        protocol = {**study.protocol(), "archive_root": str(archive_root), "image_lock": frozen}
        study.immutable(study.RESULTS / "protocol.json", protocol)
        freeze_source_bundle(archive_root)
        rows = collapsed_rows(study.RESULTS)
        if any(row["status"] not in {"ok", failures.STATUS} for row in rows):
            raise ValueError("an interrupted/invalid attempt needs inspection; automatic replacement is disabled")
        excluded = [row for row in rows if row["status"] == failures.STATUS]
        replacements = Counter((row["cell"], int(row["seed"])) for row in excluded)
        main_plan = {(r["cell"], int(r["seed"])): r for r in study.plan("main")}
        for row in excluded:
            key = row["cell"], int(row["seed"])
            if (row["phase"] != "main" or replacements[key] > failures.MAX_REPLACEMENTS
                    or key not in main_plan or row["config_sha256"] != main_plan[key]["config_sha256"]):
                raise ValueError("technical exclusion budget or phase is invalid")
            failures.verify_excluded(row, protocol)
        selected = [row for row in rows if row["phase"] == phase and row["status"] == "ok"]
        if selected and not resume:
            raise ValueError("campaign already has attempts; use --resume after inspection")
        done = set()
        for row in selected:
            planned = planned_trials.get((row["cell"], int(row["seed"])))
            if planned is None or row["config_sha256"] != planned["config_sha256"] or (row["cell"], int(row["seed"])) in done:
                raise ValueError("manifest has an unexpected/duplicate cell or changed configuration")
            verify_completed(row)
            done.add((row["cell"], int(row["seed"])))
        if phase == "main":
            pilot_rows = [row for row in rows if row["phase"] == "pilot"]
            pilot_plan = {(row["cell"], str(row["seed"])): row for row in study.plan("pilot")}
            if any((r["cell"], r["seed"]) not in pilot_plan or r["config_sha256"] != pilot_plan[(r["cell"], r["seed"])]["config_sha256"] for r in pilot_rows):
                raise ValueError("pilot cohort is not the frozen preparation plan")
            pilots = [verify_completed(row) for row in pilot_rows]
            if {m["trial"]["cell"] for m in pilots} != set(study.CELLS) or len(pilots) != 8:
                raise ValueError("all eight independent 12-round pilots must pass before main")
            if any(not m["evidence"]["technical_valid"] or not m["evidence"]["endpoint_complete"] for m in pilots):
                raise ValueError("pilot measurement/endpoint coverage is incomplete")
            remaining = 80 - len(done)
            projected = sum(m["archive_bytes"] for m in pilots) / 8 * remaining * 1.5
            if shutil.disk_usage(archive_root.parent).free < projected + 20_000_000_000:
                raise ValueError("pilot-based archive forecast leaves less than 20 GB reserve")
        staging = study.OPERATIONS / "staging"
        staging.mkdir(parents=True, exist_ok=True)
        logs = study.OPERATIONS / "logs"
        logs.mkdir(exist_ok=True)
        pending = deque(enumerate(plan, 1))
        while pending:
            index, row = pending.popleft()
            if (row["cell"], row["seed"]) in done:
                continue
            if study.sources() != frozen["sources"]:
                raise ValueError("source changed during campaign; refusing mixed-version trials")
            if archive_root.parent.stat().st_dev != archive_device:
                raise ValueError("archive volume changed; refusing the next run")
            if study.sha(row["config"]) != row["config_sha256"]:
                raise ValueError("frozen configuration changed")
            check_storage(staging, archive_root.parent)
            run_id = f"factorial-{phase}-{row['cell']}-s{row['seed']}-{secrets.token_hex(5)}"
            trial = dict(row, phase=phase, run_id=run_id)
            directory = staging / run_id
            directory.mkdir(exist_ok=False)
            shutil.copyfile(row["config"], directory / "config.yaml")
            if study.sha(directory / "config.yaml") != row["config_sha256"]:
                raise ValueError("staged configuration did not match the frozen input")
            study.immutable(directory / "input-lock.json", dict(config_sha256=row["config_sha256"],
                sources=frozen["sources"], images=frozen["images"]))
            log_path = logs / f"{run_id}.log"
            began = time.monotonic()
            committed = False
            append(study.RESULTS, dict(trial, status="running"))
            print(f"[{index}/{total}] starting {run_id}", flush=True)
            try:
                env = dict(os.environ, PYTHONPATH=str(study.ROOT / "src"), PYTHONUNBUFFERED="1")
                command = [sys.executable, "-m", "eclipse_hivemind.cli", "--config-path", str(directory / "config.yaml"),
                           "--run-id", run_id, "--output-root", str(staging), "--capture-logs", "--wait-for-exit"]
                run_logged(command, log_path, env)
                print(f"[{index}/{total}] checking evidence and container exits", flush=True)
                if archive_root.parent.stat().st_dev != archive_device:
                    raise ValueError("archive volume changed during the run; staged data retained")
                evidence = analyse_path(directory, trial)
                records = [json.loads(path.read_text()) for path in (directory / "container-logs").glob("*.json")]
                validate_container_records(records, run_id, frozen["images"])
                if study.sha(directory / "config.yaml") != row["config_sha256"]:
                    raise ValueError("raw configuration changed during training")
                if not evidence["technical_valid"]:
                    raise ValueError("measurement invalid: " + evidence["validity_reasons"])
                print(f"[{index}/{total}] archiving and verifying raw files", flush=True)
                target, sidecar, metadata = archive_run(directory, archive_root, trial, evidence)
                append(study.RESULTS, dict(trial, status="ok", seconds=round(time.monotonic() - began, 2),
                       raw_bytes=metadata["raw_bytes"], archive=str(target), metadata=str(sidecar), archive_sha256=metadata["archive_sha256"]))
                committed = True
                # Only this new run's staged duplicate, after complete verified archive.
                if directory.parent.resolve() != staging.resolve() or directory.is_symlink():
                    raise ValueError("refusing unsafe staged-copy cleanup")
                try:
                    shutil.rmtree(directory)
                except OSError as error:
                    # The archive is committed; a local cleanup failure cannot
                    # invalidate the measurement or justify repeating training.
                    print(f"Local staged copy retained: {directory}: {error}", flush=True)
                print(f"[{index}/{total}] archived and verified {run_id}", flush=True)
            except BaseException as error:
                if not committed:
                    append(study.RESULTS, dict(trial, status="interrupted" if isinstance(error, KeyboardInterrupt) else "invalid",
                           seconds=round(time.monotonic() - began, 2), detail=str(error)))
                    key = row["cell"], row["seed"]
                    if (phase == "main" and isinstance(error, subprocess.CalledProcessError)
                            and replacements[key] < failures.MAX_REPLACEMENTS):
                        try:
                            if archive_root.parent.stat().st_dev != archive_device:
                                raise ValueError("archive volume changed")
                            if study.sha(directory / "config.yaml") != row["config_sha256"]:
                                raise ValueError("failed trial configuration changed")
                            inputs = json.loads((directory / "input-lock.json").read_text())
                            verify_trial_inputs(inputs, {"image_lock": frozen}, trial)
                            target, sidecar, metadata = failures.archive_exclusion(directory, archive_root, trial, frozen["images"])
                        except (ValueError, OSError) as diagnosis:
                            print(f"Technical replacement not permitted: {diagnosis}", flush=True)
                            raise error from diagnosis
                        append(study.RESULTS, dict(trial, status=failures.STATUS, detail=metadata["exclusion"]["reason"],
                            seconds=round(time.monotonic() - began, 2), archive=str(target), metadata=str(sidecar),
                            archive_sha256=metadata["archive_sha256"], raw_bytes=metadata["raw_bytes"]))
                        replacements[key] += 1
                        pending.appendleft((index, row))
                        print(f"[{index}/{total}] excluded and archived {metadata['exclusion']['reason']}; "
                              f"repeating the same cell/seed ({replacements[key]}/{failures.MAX_REPLACEMENTS}).", flush=True)
                        try:
                            shutil.rmtree(directory)
                        except OSError as cleanup_error:
                            print(f"Archived failed stage retained: {directory}: {cleanup_error}", flush=True)
                        continue
                raise
        if phase == "pilot":
            summary = pilot_forecast(collapsed_rows(study.RESULTS))
            study.immutable(study.RESULTS / "pilot-readiness.json", summary)
            print(json.dumps(summary, indent=2), flush=True)


def collapsed_rows(root):
    latest = {}
    for row in manifest_rows(root):
        latest[row["run_id"]] = row
    return list(latest.values())


def main():
    parser = argparse.ArgumentParser(description="Run an independent SC campaign")
    parser.add_argument("--phase", choices=("pilot", "main"), default="pilot")
    parser.add_argument("--prepare-only", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--archive-root", type=Path, default=study.ARCHIVE)
    args = parser.parse_args()
    study.ARCHIVE = args.archive_root.expanduser().resolve()
    protected = [study.ROOT / name for name in ("src", "configs", "data", "scripts", "checks", "docker", "outputs")]
    if any(study.ARCHIVE == p or study.ARCHIVE.is_relative_to(p) for p in protected):
        parser.error("archive destination must be outside source, measured data and output directories")
    if args.dry_run:
        print(json.dumps(dict(phase=args.phase, rounds=study.ROUNDS, trials=study.plan(args.phase)), indent=2))
        return
    if args.prepare_only:
        study.prepare_configs()
        print("Verified 80 main and 8 pilot configurations; no containers started.")
        return
    run(args.phase, study.ARCHIVE, args.resume)
