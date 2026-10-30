"""A native teardown exclusion must be reproducible, narrow and bounded."""
from contextlib import ExitStack, redirect_stdout
import copy
import csv
import io
import json
from pathlib import Path
import subprocess
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from eclipse_hivemind.factorial import failures, study, workflow, report
from checks.test_factorial_campaign import fixture
from checks import test_factorial_workflow as workflow_fixtures


def write_trial(directory, trial, records, teardown=False):
    records = copy.deepcopy(records)
    node = "normal-10"
    if teardown:
        records = [r for r in records if not (r["node"]["node_id"] == node and
                   (r["event"]["event_type"] == "node_finished" or r["event"]["data"].get("kind") == "telemetry_complete"))]
        payload = [r["event"]["data"] for r in records if r["node"]["node_id"] == node
                   and r["event"]["event_type"] == "eclipse_observation"]
        snapshot = dict(kind="storage_snapshot", process_id=11, trace_sequence=1)
        records.append(dict(experiment_id=trial["run_id"], node=dict(node_id=node),
            event=dict(event_type="eclipse_observation", data=snapshot, occurred_at="2026-10-18T10:00:00+00:00")))
        spool = [*payload, dict(_trace_transport="finished", process_id=1, trace_sequence=len(payload), delivery_errors=0),
                 dict(_trace_transport="started", process_id=11, trace_sequence=0), snapshot,
                 dict(_trace_transport="started", process_id=42, trace_sequence=0),
                 dict(_trace_transport="finished", process_id=42, trace_sequence=1, delivery_errors=0)]
        if teardown == "drain":
            spool.append(dict(_trace_transport="finished", process_id=11, trace_sequence=2, delivery_errors=0))
            records = [r for r in records if not (r["node"]["node_id"] == node and
                       r["event"]["data"].get("kind") == "gradient_transform_summary")]
            signature, missing = "Optimizer is shut down\n", 1
        else:
            signature = ("multiprocessing/connection.py rebuild_connection resource_sharer.py\n"
                         "FileNotFoundError: [Errno 2] No such file or directory\n"
                         "DHT did not shut down within the grace period; terminating it the hard way\n")
            missing = 11
        log = (signature +
               "".join("ECLIPSE_SPOOL=" + json.dumps(d) + "\n" for d in spool) +
               f"RuntimeError: incomplete telemetry: errors=[], missing_producer_ends=[{missing}]\n")
    else:
        log = "clean trainer\n"
    (directory / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
    destination = directory / "container-logs"
    destination.mkdir(exist_ok=True)
    artifacts, images = workflow_fixtures.ArtifactTests().records()
    for artifact in artifacts:
        artifact["name"] = trial["run_id"] + artifact["name"][3:]
        if teardown and artifact["name"] == trial["run_id"] + "-" + node:
            artifact["state"]["ExitCode"] = 1
        (destination / (artifact["name"] + ".json")).write_text(json.dumps(artifact))
    (destination / (trial["run_id"] + "-" + node + ".log")).write_text(log)
    return images


class ClassificationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.directory = Path(self.temporary.name)
        self.trial, records = fixture()
        self.trial["phase"] = "main"
        self.trial["config_sha256"] = "fixture-config"
        self.images = write_trial(self.directory, self.trial, records, teardown=True)

    def test_audited_failure_stays_invalid_and_archive_is_verified(self):
        audit, evidence = failures.classify(self.directory, self.trial, self.images)
        self.assertFalse(evidence["technical_valid"])
        self.assertTrue(audit["fallback_payload_matches_logger"])
        target, sidecar, metadata = failures.archive_exclusion(
            self.directory, self.directory.parent / (self.directory.name + "-archives"), self.trial, self.images)
        self.addCleanup(shutil.rmtree, target.parent)
        row = dict(self.trial, archive=str(target), metadata=str(sidecar), config_sha256="fixture-config",
                   archive_sha256=metadata["archive_sha256"], status=failures.STATUS, detail=failures.REASON)
        failures.verify_excluded(row)
        row["run_id"] = "different"
        with self.assertRaises(ValueError):
            failures.verify_excluded(row)

    def test_learning_values_and_capture_outcomes_do_not_select_replacement(self):
        for capture, exchange in ((True, "success"), (True, "failed"), (False, "unknown")):
            trial, records = fixture(capture=capture, exchange=exchange)
            for record in records:
                if record["event"]["event_type"] == "eval_metrics":
                    record["event"]["data"].update(eval_loss=1000., eval_accuracy=0.)
            write_trial(self.directory, self.trial, records, teardown=True)
            failures.classify(self.directory, self.trial, self.images)

    def test_complete_spools_with_unforwarded_summary_identify_the_terminal_drain_race(self):
        _, records = fixture()
        write_trial(self.directory, self.trial, records, teardown="drain")
        audit, evidence = failures.classify(self.directory, self.trial, self.images)
        self.assertEqual(audit["reason"], failures.DRAIN_REASON)
        self.assertEqual(audit["unforwarded_tail_observations"], 1)
        self.assertTrue(audit["all_spool_ends_present"])
        self.assertFalse(evidence["technical_valid"])
        self.assertIn("missing_or_extra_transform_summaries", evidence["validity_reasons"])
        log = self.directory / "container-logs/fixture-normal-10.log"
        original = log.read_text()
        # A complete spool with a different payload is not evidence for the
        # EOF/stop race, even when every producer has a terminal marker.
        log.write_text(original.replace('"gradient_behavior": "standard"', '"gradient_behavior": "corrupted"'))
        with self.assertRaises(ValueError):
            failures.classify(self.directory, self.trial, self.images)
        log.write_text(original.replace('"trace_sequence": 2, "delivery_errors": 0}', '"trace_sequence": 2, "delivery_errors": 1}'))
        with self.assertRaises(ValueError):
            failures.classify(self.directory, self.trial, self.images)

    def test_missing_native_end_cannot_be_reclassified_as_a_terminal_drain_race(self):
        log = self.directory / "container-logs/fixture-normal-10.log"
        text = log.read_text()
        text = text.replace("multiprocessing/connection.py rebuild_connection resource_sharer.py", "Optimizer is shut down")
        text = text.replace("DHT did not shut down within the grace period; terminating it the hard way", "")
        log.write_text(text.replace("missing_producer_ends=[11]", "missing_producer_ends=[1]"))
        with self.assertRaises(ValueError):
            failures.classify(self.directory, self.trial, self.images)

    def test_missing_endpoint_payload_gap_unrelated_errors_and_wrong_images_stop(self):
        events = self.directory / "events.jsonl"
        log = self.directory / "container-logs/fixture-normal-10.log"
        original_events, original_log = events.read_text(), log.read_text()
        mutations = {
            "endpoint": lambda: events.write_text("\n".join(l for l in original_events.splitlines()
                if not ('"eval_metrics"' in l and '"normal-10"' in l))),
            "signature": lambda: log.write_text(original_log.replace("rebuild_connection", "other_failure")),
            "spool": lambda: log.write_text(original_log.replace('"trace_sequence": 1}', '"trace_sequence": 2}')),
            "delivery": lambda: log.write_text(original_log.replace('"delivery_errors": 0', '"delivery_errors": 1')),
            "payload": lambda: events.write_text(original_events.replace('"kind": "storage_snapshot"', '"kind": "other"')),
        }
        for name, mutate in mutations.items():
            events.write_text(original_events)
            log.write_text(original_log)
            mutate()
            with self.subTest(name=name), self.assertRaises(ValueError):
                failures.classify(self.directory, self.trial, self.images)
        events.write_text(original_events)
        log.write_text(original_log)
        for field, value in (("OOMKilled", True), ("ExitCode", 137), ("Running", True)):
            path = self.directory / "container-logs/fixture-normal-10.json"
            artifact = json.loads(path.read_text())
            original = copy.deepcopy(artifact)
            artifact["state"][field] = value
            path.write_text(json.dumps(artifact))
            with self.subTest(field=field), self.assertRaises(ValueError):
                failures.classify(self.directory, self.trial, self.images)
            path.write_text(json.dumps(original))
        with self.assertRaises(ValueError):
            failures.classify(self.directory, {**self.trial, "phase": "pilot"}, self.images)
        path = self.directory / "container-logs/fixture-normal-10.json"
        artifact = json.loads(path.read_text())
        artifact["image_id"] = "wrong-image"
        path.write_text(json.dumps(artifact))
        with self.assertRaises(ValueError):
            failures.classify(self.directory, self.trial, self.images)


class ReplacementWorkflowTests(unittest.TestCase):
    def exercise(self, failures_before_success):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as patches, redirect_stdout(io.StringIO()):
            root = Path(temporary)
            for name, value in dict(CONFIGS=root/"configs", RESULTS=root/"results", OPERATIONS=root/"operations",
                                    ARCHIVE=root/"archives/raw").items():
                patches.enter_context(patch.object(study, name, value))
            study.ARCHIVE.parent.mkdir(parents=True)
            frozen = dict(images=workflow_fixtures.ArtifactTests().records()[1], sources=study.sources())
            patches.enter_context(patch.object(workflow, "image_lock", return_value=frozen))
            patches.enter_context(patch.object(workflow, "verify_images"))
            patches.enter_context(patch.object(workflow.shutil, "disk_usage", return_value=Mock(free=100_000_000_000)))
            launched = []

            def simulate(command, log_path, env):
                run_id = command[command.index("--run-id") + 1]
                directory = Path(command[command.index("--config-path") + 1]).parent
                config = yaml.safe_load((directory / "config.yaml").read_text())
                params = config["node_types"]["victim"]["parameters"]
                cell = f"c{int(params['factor_coordination'])}-f{int(params['factor_filtering'])}-p{int(params['factor_poisoning'])}"
                phase = "main" if "-main-" in run_id else "pilot"
                trial = dict(cell=cell, seed=config["experiment"]["seed"], run_id=run_id, phase=phase)
                fail = phase == "main" and sum("-main-" in r for r in launched) < failures_before_success
                launched.append(run_id)
                mode = "drain" if fail and sum("-main-" in r for r in launched) == 2 else fail
                write_trial(directory, trial, workflow_fixtures.cell_records(cell, run_id, 0), teardown=mode)
                log_path.write_text("synthetic launcher\n")
                if fail:
                    raise subprocess.CalledProcessError(1, command)

            patches.enter_context(patch.object(workflow, "run_logged", side_effect=simulate))
            workflow.run("pilot", study.ARCHIVE)
            if failures_before_success > failures.MAX_REPLACEMENTS:
                with self.assertRaises(subprocess.CalledProcessError):
                    workflow.run("main", study.ARCHIVE, resume=True)
                rows = workflow.collapsed_rows(study.RESULTS)
                self.assertEqual(sum(r["status"] == failures.STATUS for r in rows), 2)
                self.assertEqual({r["detail"] for r in rows if r["status"] == failures.STATUS}, failures.REASONS)
                self.assertEqual(sum(r["status"] == "invalid" for r in rows), 1)
                with self.assertRaisesRegex(ValueError, "needs inspection"):
                    workflow.run("main", study.ARCHIVE, resume=True)
                self.assertEqual(len(launched), 11)
            else:
                workflow.run("main", study.ARCHIVE, resume=True)
                workflow.run("main", study.ARCHIVE, resume=True)
                self.assertEqual(len(launched), 90)
                rows = workflow.collapsed_rows(study.RESULTS)
                self.assertEqual(sum(r["status"] == failures.STATUS for r in rows), 2)
                self.assertEqual(sum(r["status"] == "ok" and r["phase"] == "main" for r in rows), 80)
                report.export(study.RESULTS, title_prefix="SYNTHETIC TEST — ")
                self.assertIn("2 additional attempts", (study.RESULTS / "REPORT.md").read_text())
                with (study.RESULTS / "data/technical-exclusions.csv").open() as stream:
                    self.assertEqual(len(list(csv.DictReader(stream))), 2)

    def test_two_exclusions_resume_and_eighty_valid_results_are_all_reported(self):
        self.exercise(2)

    def test_third_failure_stops_and_is_not_silently_retried_on_resume(self):
        self.exercise(3)


if __name__ == "__main__":
    unittest.main()
