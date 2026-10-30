"""Offline adversarial cases; no Docker image, network swarm or training starts."""
from __future__ import annotations

import copy
import io
import json
import multiprocessing
import os
from pathlib import Path
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "src/eclipse_hivemind/nodes")]
from eclipse_hivemind.factorial import study, evidence, workflow, report
from factorial.trace import Trace


def produce(trace, count, finish=True):
    trace.producer_started()
    for index in range(count):
        trace.emit("probe", index=index)
    if finish:
        trace.producer_finished()


class TransportTests(unittest.TestCase):
    def trace(self):
        return Trace(dict(experiment_id="probe", node_id="probe", node_type="probe", node_index=0), "http://unused")

    def test_concurrent_processes_preserve_every_record_and_terminal_audit(self):
        trace = self.trace()
        received = []
        with patch("factorial.trace.client.send_eclipse_observations", side_effect=lambda **kw: received.extend(kw["observations"]) or {"accepted": len(kw["observations"])}):
            for _ in range(20):
                process = multiprocessing.get_context("fork").Process(target=produce, args=(trace, 1200))
                process.start()
                trace.register_process(process)
            trace.start()
            trace.emit("parent")
            self.assertEqual(trace.close(), [])
        self.assertEqual(sum(r["kind"] == "probe" for r in received), 24000)
        self.assertEqual(received[-1]["kind"], "telemetry_complete")
        self.assertEqual(received[-1]["producer_count"], 21)
        self.assertEqual(received[-1]["acknowledged_observations"], 24001)
        self.assertFalse(trace.directory.exists())

    def test_missing_terminal_marker_is_not_reclassified_as_complete(self):
        trace = self.trace()
        with patch("factorial.trace.client.send_eclipse_observations", side_effect=lambda **kw: {"accepted": len(kw["observations"])}):
            process = multiprocessing.get_context("fork").Process(target=produce, args=(trace, 3, False))
            process.start()
            trace.register_process(process)
            with self.assertRaisesRegex(RuntimeError, "missing_producer_ends"):
                trace.close()
        self.assertTrue(trace.directory.exists())

    def test_bad_acknowledgement_fails_measurement(self):
        trace = self.trace()
        trace.emit("probe")
        with patch("factorial.trace.client.send_eclipse_observations", return_value={"accepted": 0}):
            with self.assertRaisesRegex(RuntimeError, "incomplete telemetry"):
                trace.close()

    def test_observation_after_end_is_an_error(self):
        trace = self.trace()
        trace.producer_finished()
        with self.assertRaisesRegex(RuntimeError, "terminal"):
            trace.emit("late")
        with patch("factorial.trace.client.send_eclipse_observations", side_effect=lambda **kw: {"accepted": len(kw["observations"])}):
            trace.close()

    def test_stop_during_idle_eof_scan_still_forwards_the_final_owner_record(self):
        trace = self.trace()
        trace.emit("parent")
        idle, release = threading.Event(), threading.Event()
        original_open = Path.open
        received, errors = [], []

        class Reader:
            def __init__(self, stream):
                self.stream, self.empty_reads = stream, 0

            def __getattr__(self, name):
                return getattr(self.stream, name)

            def readline(self):
                line = self.stream.readline()
                if not line:
                    self.empty_reads += 1
                    if self.empty_reads == 2:
                        idle.set()
                        if not release.wait(5):
                            raise RuntimeError("EOF race synchronization timed out")
                return line

        def open_stream(path, *args, **kwargs):
            stream = original_open(path, *args, **kwargs)
            return Reader(stream) if path.parent == trace.directory and args == ("rb",) else stream

        def close():
            try:
                trace.emit("last_parent_record")
                trace.close()
            except BaseException as error:
                errors.append(error)

        with patch.object(Path, "open", open_stream), patch("factorial.trace.client.send_eclipse_observations",
                side_effect=lambda **kw: received.extend(kw["observations"]) or {"accepted": len(kw["observations"])}):
            trace.start()
            try:
                self.assertTrue(idle.wait(5))
                closer = threading.Thread(target=close)
                closer.start()
                self.assertTrue(trace.stop.wait(5))
            finally:
                release.set()
            closer.join(5)
            self.assertFalse(closer.is_alive())
        self.assertEqual(errors, [])
        self.assertEqual([r["kind"] for r in received], ["parent", "last_parent_record", "telemetry_complete"])
        self.assertEqual(received[-1]["acknowledged_observations"], 2)
        self.assertTrue(trace.completed)
        self.assertFalse(trace.directory.exists())


class DesignTests(unittest.TestCase):
    def test_eighty_unique_main_cells_and_disjoint_pilots(self):
        main, pilot = study.plan("main"), study.plan("pilot")
        self.assertEqual(len(main), 80)
        self.assertEqual(len(pilot), 8)
        self.assertEqual(len({(r["cell"], r["seed"]) for r in main}), 80)
        self.assertFalse({r["seed"] for r in main} & {r["seed"] for r in pilot})
        for seed in range(17, 27):
            self.assertEqual({r["cell"] for r in main if r["seed"] == seed}, set(study.CELLS))

    def test_only_intended_factor_parameters_differ(self):
        base = study.config("c0-f0-p0", 17)
        for cell in study.CELLS:
            config = study.config(cell, 17)
            self.assertEqual(config["experiment"]["rounds"], 12)
            self.assertEqual(sum(n["count"] for n in config["node_types"].values()), 20)
            self.assertEqual({n["image"] for n in config["node_types"].values()}, {study.IMAGE})
            c, f, p = study.factors(cell)
            for role, node in config["node_types"].items():
                params = node["parameters"]
                self.assertEqual(params["averager_behavior"] == "reverse-gradient", bool(p and role == "recruiter"))
                self.assertEqual(params["storage_behavior"] == "filter", bool(f and role in {"recruiter", "support"}))
                for key in ["factor_coordination", "factor_filtering", "factor_poisoning", "averager_behavior", "averager_role", "storage_behavior"]:
                    params[key] = base["node_types"][role]["parameters"][key]
            self.assertEqual(config, base)

    def test_immutable_records_cannot_change(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "record.json"
            study.immutable(path, {"a": 1})
            study.immutable(path, {"a": 1})
            with self.assertRaises(ValueError):
                study.immutable(path, {"a": 2})


def fixture(capture=True, exchange="success"):
    trial = dict(cell="c0-f0-p0", seed=17, run_id="fixture")
    records = []
    def add(node, kind, data):
        records.append(dict(experiment_id="fixture", node=dict(node_id=node), event=dict(event_type=kind, data=data,
                       occurred_at="2026-10-18T10:00:00+00:00")))
    selected = [f"peer-recruiter-{i}" for i in range(3)]
    for node in sorted(evidence.expected_nodes()):
        add(node, "node_started", dict(model_fingerprint="same-initial-model", runtime_metadata=dict(peer_id="peer-" + node, attacker_controlled=node.startswith(("recruiter", "support")),
            placement="targeted" if node.startswith(("recruiter", "support")) else "random", storage_role="honest",
            averager_role="controlled" if node.startswith("recruiter") else "honest", factorial_runtime_version="factorial-v1-20261018")))
    counts = {}
    def trace(node, kind, **data):
        sequence = counts.get(node, 0)
        counts[node] = sequence + 1
        add(node, "eclipse_observation", dict(kind=kind, process_id=1, trace_sequence=sequence, **data))
    for node in sorted(evidence.expected_nodes()):
        trace(node, "factorial_conditions", coordination=False, filtering=False, poisoning=False,
              gradient_behavior="standard", selected_peer_ids=selected, victim_peer_id="peer-victim-0")
        trace(node, "gradient_transform_summary", reversed=False, calls=0)
        for n in (0, 12):
            add(node, "eval_metrics", dict(round=n, eval_loss=1.0, eval_accuracy=.5, predicted_class_fractions=[.25] * 4, samples=4096))
    trace("victim-0", "candidate_snapshot", candidates=[dict(peer_id=peer) for peer in selected], query_id="q")
    members = ["peer-victim-0", *selected] if capture else ["peer-victim-0", "peer-normal-0"]
    trace("victim-0", "group_formed", group_id="g", members=[dict(peer_id=peer) for peer in members], leader_peer_id=members[1], dht_time=1792317610.)
    if exchange != "unknown":
        trace("victim-0", "allreduce_completed", group_id="g", success=exchange == "success")
    for peer in members:
        trace(peer.removeprefix("peer-"), "averaging_contribution", group_id="g", peer_id=peer, weight=32)
    for node in sorted(evidence.expected_nodes()):
        add(node, "eclipse_observation", dict(kind="telemetry_complete", transport_version="process_spool_v4",
            acknowledged_observations=counts[node], producer_count=1, producer_record_counts={"1": counts[node] + 1}))
        add(node, "node_finished", {})
    return trial, records


class EvidenceTests(unittest.TestCase):
    def analyse(self, records, trial=None):
        return evidence.analyse((json.dumps(r) for r in records), trial or dict(cell="c0-f0-p0", seed=17, run_id="fixture"))

    def test_full_capture_is_all_partners_but_not_all_group_members(self):
        trial, records = fixture()
        result = self.analyse(records, trial)
        self.assertTrue(result["technical_valid"], result["validity_reasons"])
        self.assertEqual(result["partner_share"], 1)
        self.assertEqual(result["recruiter_leader_rate"], 1)
        self.assertEqual(result["recruiter_weight_share"], .75)
        self.assertEqual(result["successful_capture_fraction"], 1)

    def test_unknown_exchange_is_not_success(self):
        trial, records = fixture(exchange="unknown")
        result = self.analyse(records, trial)
        self.assertTrue(result["technical_valid"], result["validity_reasons"])
        self.assertEqual(result["full_capture_fraction"], 1)
        self.assertEqual(result["successful_capture_fraction"], 0)
        self.assertEqual(result["unknown_allreduces"], 1)
        self.assertTrue(result["first_capture_censored"])

    def test_no_group_is_valid_and_fraction_undefined(self):
        trial, records = fixture()
        # Remove all per-group traces and rebuild producer sequence audit.
        for r in records:
            if r["event"]["event_type"] == "eclipse_observation" and r["event"]["data"].get("kind") == "group_formed":
                r["event"]["data"]["members"] = [{"peer_id": "peer-normal-0"}]
        result = self.analyse(records, trial)
        self.assertTrue(result["technical_valid"], result["validity_reasons"])
        self.assertEqual(result["victim_groups"], 0)
        self.assertIsNone(result["partner_share"])

    def test_invalid_observed_weight_remains_a_measurement_failure(self):
        trial, records = fixture()
        for r in records:
            d = r["event"]["data"]
            if d.get("kind") == "averaging_contribution":
                d["weight"] = None
                break
        result = self.analyse(records, trial)
        self.assertFalse(result["technical_valid"])
        self.assertIn("invalid_averaging_weight", result["validity_reasons"])

    def test_unobserved_member_keeps_coverage_unknown_without_invalidating_complete_telemetry(self):
        trial, records = fixture()
        for record in records:
            data = record["event"]["data"]
            if data.get("kind") == "averaging_contribution" and record["node"]["node_id"] != "victim-0":
                data["kind"] = "producer_probe"
                break
        groups = []
        result = evidence.analyse((json.dumps(r) for r in records), trial,
            lambda table, row: groups.append(row) if table == "groups" else None)
        self.assertTrue(result["technical_valid"], result["validity_reasons"])
        self.assertEqual(result["successful_groups_with_incomplete_weights"], 1)
        self.assertEqual(result["weight_member_coverage"], .75)
        self.assertIsNone(result["recruiter_weight_share"])
        self.assertIsNone(groups[0]["weight_total"])
        self.assertFalse(groups[0]["weights_complete"])
        self.assertEqual(groups[0]["observed_weight_total"], 96)
        self.assertEqual(len(json.loads(groups[0]["missing_weight_peers"])), 1)

    def test_missing_transport_is_not_excused_by_partial_weight_coverage(self):
        trial, records = fixture()
        records = [r for r in records if not (r["node"]["node_id"] == "recruiter-0"
                   and r["event"]["data"].get("kind") == "telemetry_complete")]
        result = self.analyse(records, trial)
        self.assertFalse(result["technical_valid"])
        self.assertIn("missing_or_extra_transport_audits", result["validity_reasons"])

    def test_idle_child_with_only_start_end_markers_is_complete(self):
        trial, records = fixture()
        for record in records:
            data = record["event"]["data"]
            if data.get("kind") == "telemetry_complete":
                data["producer_record_counts"]["2"] = 2
                data["producer_count"] = 2
        result = self.analyse(records, trial)
        self.assertTrue(result["technical_valid"], result["validity_reasons"])

    def test_unobserved_child_payload_is_not_mistaken_for_idle_child(self):
        trial, records = fixture()
        for record in records:
            data = record["event"]["data"]
            if data.get("kind") == "telemetry_complete":
                data["producer_record_counts"]["2"] = 3
                data["producer_count"] = 2
                break
        result = self.analyse(records, trial)
        self.assertFalse(result["technical_valid"])
        self.assertIn("producer_tail_unconfirmed", result["validity_reasons"])

    def test_complete_zero_weights_are_valid_with_undefined_relative_share(self):
        trial, records = fixture()
        groups = []
        for record in records:
            data = record["event"]["data"]
            if data.get("kind") == "averaging_contribution":
                data["weight"] = 0.0
        result = evidence.analyse((json.dumps(r) for r in records), trial,
            lambda table, row: groups.append(row) if table == "groups" else None)
        self.assertTrue(result["technical_valid"], result["validity_reasons"])
        self.assertEqual(result["successful_zero_weight_groups"], 1)
        self.assertIsNone(result["recruiter_weight_share"])
        self.assertTrue(groups[0]["weights_complete"])
        self.assertEqual(groups[0]["weight_total"], 0)
        self.assertTrue(groups[0]["zero_weight_group"])

    def test_wrong_contributor_identity_with_zero_weights_is_still_invalid(self):
        trial, records = fixture()
        first = True
        for record in records:
            data = record["event"]["data"]
            if data.get("kind") == "averaging_contribution":
                data["weight"] = 0.0
                if first:
                    data["peer_id"] = "unrelated-peer"
                    first = False
        result = self.analyse(records, trial)
        self.assertFalse(result["technical_valid"])
        self.assertIn("contribution_peer_identity_mismatch", result["validity_reasons"])

    def test_treatment_mismatch_is_detected(self):
        trial, records = fixture()
        for r in records:
            if r["event"]["data"].get("kind") == "factorial_conditions":
                r["event"]["data"]["poisoning"] = True
                break
        self.assertIn("wrong_factor_delivery", self.analyse(records, trial)["validity_reasons"])


class ArchiveTests(unittest.TestCase):
    def test_incomplete_preparation_does_not_claim_a_forecast_or_pass(self):
        result = workflow.pilot_forecast([])
        self.assertFalse(result["passed"])
        self.assertIsNone(result["projected_main_hours_including_archive"])

    def test_all_raw_files_roundtrip_and_corruption_is_detected(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            source = root / "stage"
            source.mkdir()
            (source / "events.jsonl").write_text('{"a": 1}\n')
            (source / "events.sqlite3").write_bytes(b"arbitrary fixture database bytes")
            target, sidecar, metadata = workflow.archive_run(source, root / "archive", {"run_id": "fixture"}, {})
            workflow.verify_archive(target, metadata)
            self.assertEqual(set(metadata["members"]), {"events.jsonl", "events.sqlite3"})
            self.assertTrue(source.exists())
            with target.open("ab") as stream:
                stream.write(b"corruption")
            with self.assertRaisesRegex(ValueError, "checksum"):
                workflow.verify_archive(target, metadata)

    def test_manifest_uses_terminal_state_not_running_transition(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            workflow.append(root, dict(run_id="run", status="running"))
            workflow.append(root, dict(run_id="run", status="ok"))
            self.assertEqual(workflow.collapsed_rows(root)[0]["status"], "ok")

    def test_unfinished_manifest_cannot_produce_a_final_report(self):
        with tempfile.TemporaryDirectory() as folder:
            with self.assertRaisesRegex(ValueError, "80 unique"):
                report.export(Path(folder))

    def test_eighty_archives_recompute_to_three_figures_and_flat_report(self):
        with tempfile.TemporaryDirectory() as folder:
            root = Path(folder)
            frozen = dict(images={"node": "synthetic-only"}, sources=study.sources())
            study.immutable(root / "protocol.json", dict(study.protocol(), image_lock=frozen))
            for planned in study.plan("main"):
                c, f, p = study.factors(planned["cell"])
                original, records = fixture(capture=bool(c))
                trial = dict(planned, phase="main", run_id=f"synthetic-{planned['cell']}-{planned['seed']}")
                for record in records:
                    record["experiment_id"] = trial["run_id"]
                    node, d = record["node"]["node_id"], record["event"]["data"]
                    if record["event"]["event_type"] == "node_started":
                        d["runtime_metadata"]["storage_role"] = "malicious" if f and node.startswith(("recruiter", "support")) else "honest"
                        if p and node.startswith("recruiter"):
                            d["runtime_metadata"]["averager_role"] = "malicious"
                    if d.get("kind") == "factorial_conditions":
                        d.update(coordination=bool(c), filtering=bool(f), poisoning=bool(p),
                                 gradient_behavior="reverse-gradient" if p and node.startswith("recruiter") else "standard")
                    if d.get("kind") == "gradient_transform_summary" and p and node.startswith("recruiter"):
                        d.update(reversed=True, calls=1)
                raw = root / "stage" / trial["run_id"]
                raw.mkdir(parents=True)
                (raw / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
                (raw / "config.yaml").write_text(__import__("yaml").safe_dump(study.config(trial["cell"], trial["seed"]), sort_keys=False))
                study.immutable(raw / "input-lock.json", dict(frozen, config_sha256=trial["config_sha256"]))
                result = evidence.analyse_path(raw, trial)
                self.assertTrue(result["technical_valid"], result["validity_reasons"])
                target, sidecar, metadata = workflow.archive_run(raw, root / "archives", trial, result)
                workflow.append(root, dict(trial, status="ok", archive=str(target), metadata=str(sidecar), archive_sha256=metadata["archive_sha256"]))
            report.export(root, title_prefix="SYNTHETIC TEST — ")
            self.assertTrue((root / "REPORT.md").exists())
            self.assertEqual(len(list((root / "figures").glob("*.png"))), 3)
            self.assertEqual({p.suffix for p in (root / "figures").iterdir()}, {".png"})
            self.assertTrue(json.loads((root / "analysis-audit.json").read_text())["synthetic"])
            with (root / "data/trials.csv").open() as stream:
                self.assertEqual(len(list(__import__("csv").DictReader(stream))), 80)


if __name__ == "__main__":
    unittest.main()
