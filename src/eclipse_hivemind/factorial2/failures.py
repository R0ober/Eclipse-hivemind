"""Audit two completed-training shutdown failures without accepting their trials."""
from __future__ import annotations

from collections import defaultdict
import copy
import hashlib
import json
from pathlib import Path
import re
import tarfile

from . import study
from .evidence import analyse_path, expected_nodes

REASON = "dht_resource_sharer_teardown"
DRAIN_REASON = "terminal_spool_drain_race"
SHUTDOWN_REASON = "completed_training_dht_shutdown_timeout"
REASONS = {REASON, DRAIN_REASON, SHUTDOWN_REASON}
MAX_REPLACEMENTS = 2
STATUS = "excluded_technical"


def classify(directory, trial, images):
    """Return an exclusion audit only for a verified shutdown failure.

    Native DHT termination requires exact spool/logger payload equality. The
    terminal drain race requires all spool ends and a matching logged prefix.
    Missing logger tails are preserved in raw logs, never added to measurements.
    Learning values and group outcomes are deliberately not selection criteria.
    """
    from .workflow import validate_container_records

    if trial["phase"] != "main":
        raise ValueError("technical replacement is restricted to main trials")
    evidence = analyse_path(directory, trial)
    reasons = set(evidence["validity_reasons"].split("|"))
    required = {"missing_or_extra_finishes", "missing_or_extra_transport_audits"}
    if (not required <= reasons or reasons - required - {"missing_or_extra_transform_summaries"}
            or evidence["nodes_finished"] != 19 or not evidence["endpoint_complete"]):
        raise ValueError("not a recognized completed-training shutdown failure")
    records = [json.loads(p.read_text()) for p in (directory / "container-logs").glob("*.json")]
    failed = [r for r in records if r["state"].get("ExitCode") != 0]
    if len(failed) != 1 or failed[0]["state"].get("ExitCode") != 1:
        raise ValueError("expected exactly one failed trainer with exit status 1")
    node = failed[0]["name"].removeprefix(trial["run_id"] + "-")
    if node not in expected_nodes():
        raise ValueError("the failed container is not a planned trainer")
    # Keep every identity, image, OOM and running-state check. This temporary
    # copy only lets the common validator check the other 21 clean exits.
    checked = copy.deepcopy(records)
    next(r for r in checked if r["name"] == failed[0]["name"])["state"]["ExitCode"] = 0
    validate_container_records(checked, trial["run_id"], images)
    log = (directory / "container-logs" / (failed[0]["name"] + ".log")).read_text()
    markers = ("multiprocessing/connection.py", "rebuild_connection", "resource_sharer.py",
               "FileNotFoundError: [Errno 2] No such file or directory",
               "DHT did not shut down within the grace period; terminating it the hard way")
    match = re.findall(r"RuntimeError: incomplete telemetry: errors=\[\], missing_producer_ends=(\[[0-9, ]+\])", log)
    if len(match) != 1:
        raise ValueError("terminal audit failure signature is absent or ambiguous")
    missing = set(json.loads(match[0]))
    native_exception = all(marker in log for marker in markers)
    shutdown_at = log.find("Optimizer is shut down")
    terminate_at = log.find(markers[-1])
    audit_at = log.find("RuntimeError: incomplete telemetry:")
    shutdown_timeout = (0 <= shutdown_at < terminate_at < audit_at
                        and "ECLIPSE_TELEMETRY_ERROR=" not in log
                        and len(missing) == 1 and 1 not in missing)
    native = native_exception or shutdown_timeout
    if native:
        if len(missing) != 1 or 1 in missing or reasons != required:
            raise ValueError("native DHT failure has additional missing evidence")
        reason = REASON if native_exception else SHUTDOWN_REASON
    else:
        if (1 not in missing or "Optimizer is shut down" not in log
                or "ECLIPSE_TELEMETRY_ERROR=" in log or markers[-1] in log):
            raise ValueError("neither the native DHT nor terminal drain signature is present")
        reason = DRAIN_REASON
    spools = defaultdict(list)
    for line in log.splitlines():
        if line.startswith("ECLIPSE_SPOOL="):
            item = json.loads(line.partition("=")[2])
            spools[item["process_id"]].append(item)
    if len(spools) != 3 or not missing <= spools.keys():
        raise ValueError("expected complete fallback spools for owner, DHT and averager")
    payload = {}
    for pid, items in spools.items():
        if [d["trace_sequence"] for d in items] != list(range(len(items))):
            raise ValueError("fallback spool has a sequence gap")
        ends = [d for d in items if d.get("_trace_transport") == "finished"]
        if native and pid in missing:
            if ends or items[0].get("_trace_transport") != "started" or not any(d.get("kind") == "storage_snapshot" for d in items):
                raise ValueError("missing end is not attributable to the DHT producer")
        elif len(ends) != 1 or items[-1] != ends[0] or ends[0].get("delivery_errors") != 0:
            raise ValueError("another producer did not close cleanly")
        for item in items:
            if "_trace_transport" not in item:
                payload[pid, item["trace_sequence"]] = item
    observed, finished, audited, transformed = {}, set(), set(), set()
    with (directory / "events.jsonl").open() as stream:
        for line in stream:
            record = json.loads(line)
            name, event = record["node"]["node_id"], record["event"]
            data = event["data"]
            if event["event_type"] == "node_finished":
                finished.add(name)
            if data.get("kind") == "telemetry_complete":
                audited.add(name)
            if data.get("kind") == "gradient_transform_summary":
                transformed.add(name)
            if name == node and event["event_type"] == "eclipse_observation" and data.get("kind") != "telemetry_complete":
                key = data["process_id"], data["trace_sequence"]
                if key in observed:
                    raise ValueError("duplicate failed-node observation")
                observed[key] = data
    missing_transforms = expected_nodes() - transformed
    if (expected_nodes() - finished != {node} or expected_nodes() - audited != {node}
            or missing_transforms not in (set(), {node})
            or any(key not in payload or payload[key] != value for key, value in observed.items())):
        raise ValueError("failed-node payload or completion coverage is not fully accounted for")
    if native:
        if observed != payload:
            raise ValueError("native teardown payload differs from the logger")
        return dict(reason=reason, failed_node=node, missing_producer=next(iter(missing)),
                    all_twenty_endpoints=True, fallback_payload_matches_logger=True,
                    shutdown_after_optimizer=shutdown_timeout,
                    original_trial_remains_invalid=True), evidence
    for pid in spools:
        keys = [key for key in payload if key[0] == pid]
        logged = [key for key in keys if key in observed]
        if logged != keys[:len(logged)]:
            raise ValueError("logged producer records are not a contiguous spool prefix")
    return dict(reason=reason, failed_node=node, missing_producers=sorted(missing),
                all_twenty_endpoints=True, all_spool_ends_present=True,
                producer_record_counts={str(pid): len(items) for pid, items in spools.items()},
                spool_observations=len(payload), logger_observations=len(observed),
                unforwarded_tail_observations=len(payload) - len(observed),
                logged_prefix_matches_spool=True, original_trial_remains_invalid=True), evidence


def verify_excluded(row, protocol=None):
    from .workflow import verify_archive, verify_trial_inputs

    metadata = json.loads(Path(row["metadata"]).read_text())
    if (row["status"] != STATUS or row.get("detail") not in REASONS
            or metadata.get("exclusion", {}).get("reason") != row.get("detail")
            or metadata["evidence"].get("technical_valid") is not False
            or metadata["archive_sha256"] != row["archive_sha256"]
            or any(str(metadata["trial"].get(k)) != str(row.get(k))
                   for k in ("run_id", "phase", "cell", "seed", "config_sha256"))):
        raise ValueError("technical exclusion identity or audit is invalid")
    verify_archive(row["archive"], metadata)
    with tarfile.open(row["archive"], "r:gz") as archive:
        if json.load(archive.extractfile("technical-exclusion.json")) != metadata["exclusion"]:
            raise ValueError("exclusion audit differs from its archived original")
        if protocol is not None:
            inputs = json.load(archive.extractfile("input-lock.json"))
            verify_trial_inputs(inputs, protocol, row)
            if (hashlib.sha256(archive.extractfile("config.yaml").read()).hexdigest() != row["config_sha256"]
                    or inputs["config_sha256"] != row["config_sha256"]):
                raise ValueError("excluded trial configuration differs from its lock")
    return metadata


def archive_exclusion(directory, archive_root, trial, images):
    from .workflow import archive_run

    audit, evidence = classify(directory, trial, images)
    # Store the audit inside the verified raw archive as well as its sidecar.
    study.immutable(directory / "technical-exclusion.json", audit)
    return archive_run(directory, archive_root, trial, evidence, exclusion=audit)
