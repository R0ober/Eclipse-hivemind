"""SC 2 runtime, evidence and complete offline campaign validation."""
from contextlib import ExitStack, redirect_stdout
import csv
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch
import yaml
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "src/eclipse_hivemind/nodes")]
from eclipse_hivemind.factorial2 import evidence, report, safety, study, workflow
from checks.test_factorial_workflow import cell_records as full_records
from checks import test_factorial_runtime as previous_runtime

def cell_records(*args):
    records=full_records(*args)
    for record in records:
        data=record["event"]["data"]
        if data.get("kind")=="telemetry_complete":
            data.update(transport_version="process_spool_compact_v1",compact_schema="rpc-counters-v1",compact_summary_records=0,compacted_events_by_kind={})
    return records

class RuntimeTests(previous_runtime.RuntimeTests):
    def setUp(self):
        super().setUp()
        spec = importlib.util.spec_from_file_location("factorial.runtime_probe", ROOT / "src/eclipse_hivemind/nodes/factorial2/runtime.py")
        self.runtime = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(self.runtime)

    def test_dht_receives_a_separate_shutdown_budget(self):
        with patch.dict(os.environ, {"PARAM_BUCKET_SIZE": "5"}):
            super().test_dht_receives_a_separate_shutdown_budget()

    def test_bucket_is_forwarded_on_every_role_and_invalid_values_never_start_a_dht(self):
        from types import SimpleNamespace
        for role in ("normal", "victim", "recruiter", "support"):
            runtime = self.runtime.FactorialRuntime(SimpleNamespace(replicas=5, placement="random"), role, False)
            dht = Mock()
            dht.run_coroutine.return_value = {}
            with patch.dict(os.environ, {"PARAM_BUCKET_SIZE": "5"}), patch.object(self.runtime, "start_dht", return_value=dht) as start:
                runtime.start_dht()
                self.assertEqual(start.call_args.kwargs["bucket_size"], 5)
            for value in ("0", "-1", "true", "5.0"):
                with patch.dict(os.environ, {"PARAM_BUCKET_SIZE": value}), patch.object(self.runtime, "start_dht") as start:
                    with self.assertRaises(ValueError):
                        runtime.start_dht()
                    start.assert_not_called()

class ConfigurationTests(unittest.TestCase):
    def test_wrong_or_missing_actual_bucket_is_a_technical_failure(self):
        trial = dict(cell="c1-f1-p1", seed=17, run_id="probe")
        records = cell_records(trial["cell"], trial["run_id"], 0)
        for r in records:
            if r["event"]["event_type"] == "node_started":
                r["event"]["data"]["runtime_metadata"]["bucket_size"] = 5
        encode = lambda: (json.dumps(r) for r in records)
        self.assertTrue(evidence.analyse(encode(), trial)["technical_valid"])
        first = next(r for r in records if r["event"]["event_type"] == "node_started")
        first["event"]["data"]["runtime_metadata"]["bucket_size"] = 20
        self.assertIn("wrong_native_bucket_size", evidence.analyse(encode(), trial)["validity_reasons"])
        del first["event"]["data"]["runtime_metadata"]["bucket_size"]
        self.assertIn("wrong_native_bucket_size", evidence.analyse(encode(), trial)["validity_reasons"])

class IsolatedPaths(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.base = Path(self.temp.name).resolve()
        self.stack = ExitStack()
        self.addCleanup(self.temp.cleanup)
        self.addCleanup(self.stack.close)
        for name, value in {
            "ROOT": self.base / "repo",
            "CONFIGS": self.base / "repo/configs/sc2",
            "RESULTS": self.base / "repo/outputs/sc2",
            "OPERATIONS": self.base / "repo/outputs/sc2-operations",
            "ARCHIVE": self.base / "archives/sc2/raw",
        }.items():
            self.stack.enter_context(patch.object(study, name, value))

class FullWorkflowTests(IsolatedPaths):
    def test_eight_pilots_eighty_mains_resume_report_and_old_data_preservation(self):
        # Real archive/resume/evidence/report functions, synthetic closed trainer
        # events, and no Docker/network/training. All temporary writes stay here.
        with patch.object(study, "ROOT", ROOT):
            actual_sources = study.sources()
        for name in actual_sources:
            target = study.ROOT / name
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes((ROOT / name).read_bytes())
        self.stack.enter_context(patch.object(study, "sources", return_value=actual_sources))
        frozen = dict(images={tag: tag + "-id" for tag in study.IMAGES.values()}, sources=actual_sources)
        self.stack.enter_context(patch.object(workflow, "image_lock", return_value=frozen))
        self.stack.enter_context(patch.object(workflow, "verify_images"))
        self.stack.enter_context(patch.object(workflow.shutil, "disk_usage", return_value=Mock(free=100_000_000_000)))
        sentinel = self.base / "unrelated-data.bin"
        sentinel.write_bytes(b"original external data")
        launched = []

        def simulate(command, log_path, env):
            run_id = command[command.index("--run-id") + 1]
            directory = Path(command[command.index("--config-path") + 1]).parent
            config = yaml.safe_load((directory / "config.yaml").read_text())
            p = config["node_types"]["victim"]["parameters"]
            cell = f"c{int(p['factor_coordination'])}-f{int(p['factor_filtering'])}-p{int(p['factor_poisoning'])}"
            records = cell_records(cell, run_id, len(launched) % 6)
            launched.append(run_id)
            for r in records:
                if r["event"]["event_type"] == "node_started":
                    r["event"]["data"]["runtime_metadata"]["bucket_size"] = 5
            (directory / "events.jsonl").write_text("".join(json.dumps(r) + "\n" for r in records))
            (directory / "events.sqlite3").write_bytes(b"opaque synthetic database")
            destination = directory / "container-logs"
            destination.mkdir()
            participants = {n: study.IMAGE for n in evidence.expected_nodes()}
            participants.update(logger=study.IMAGES["logger"], **{"attack-coordinator": study.IMAGES["coordinator"]})
            for node, tag in participants.items():
                value = dict(name=run_id + "-" + node, image_id=frozen["images"][tag],
                             state=dict(ExitCode=0, OOMKilled=False, Running=False))
                (destination / (value["name"] + ".json")).write_text(json.dumps(value))
            log_path.write_text("Synthetic fixture; no training started.\n")

        self.stack.enter_context(patch.object(workflow, "run_logged", side_effect=simulate))
        with redirect_stdout(io.StringIO()):
            workflow.run("pilot", study.ARCHIVE)
            workflow.run("main", study.ARCHIVE, resume=True)
            workflow.run("main", study.ARCHIVE, resume=True)
            report.export(study.RESULTS, title_prefix="SYNTHETIC TEST — ")
        self.assertEqual(len(launched), 88)
        self.assertEqual(sentinel.read_bytes(), b"original external data")
        self.assertEqual(len(list(study.ARCHIVE.glob("*.tar.gz"))), 88)
        self.assertEqual(list((study.OPERATIONS / "staging").iterdir()), [])
        self.assertEqual(len(list((study.RESULTS / "figures").glob("*.png"))), 3)
        self.assertEqual({p.suffix for p in (study.RESULTS / "figures").iterdir()}, {".png"})
        self.assertIn("bucket_size=5", (study.RESULTS / "REPORT.md").read_text())
        with (study.RESULTS / "data/trials.csv").open() as stream:
            trials = list(csv.DictReader(stream))
        self.assertEqual(len(trials), 80)
        self.assertTrue(any(int(r["victim_groups"]) == 0 for r in trials))
        self.assertTrue(any(int(r["successful_groups_with_incomplete_weights"]) for r in trials))
