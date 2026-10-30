"""Exercise the real launcher, archive, resume and report without training."""
from contextlib import ExitStack, redirect_stdout
import csv
import io
import json
from pathlib import Path
import shutil
import sys
import tempfile
import unittest
from unittest.mock import Mock, patch

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from eclipse_hivemind.factorial import evidence, report, study, workflow
from checks.test_factorial_campaign import fixture


def cell_records(cell, run_id, case):
    c, f, p = study.factors(cell)
    _, records = fixture(capture=True, exchange="failed" if case == 3 else "unknown" if case == 4 else "success")
    removed_weight = False
    for record in records:
        record["experiment_id"] = run_id
        node, data = record["node"]["node_id"], record["event"]["data"]
        owner, recruiter = node.startswith(("recruiter", "support")), node.startswith("recruiter")
        if record["event"]["event_type"] == "node_started":
            data["runtime_metadata"]["storage_role"] = "malicious" if f and owner else "honest"
            data["runtime_metadata"]["averager_role"] = ("malicious" if p else "controlled") if recruiter else "honest"
        if data.get("kind") == "factorial_conditions":
            data.update(coordination=bool(c), filtering=bool(f), poisoning=bool(p),
                        gradient_behavior="reverse-gradient" if p and recruiter else "standard")
        if data.get("kind") == "gradient_transform_summary":
            data.update(reversed=bool(p and recruiter), calls=int(bool(p and recruiter)))
        if data.get("kind") == "averaging_contribution":
            if case == 1:
                data["weight"] = 0.
            elif case == 2 and node != "victim-0" and not removed_weight:
                data["kind"] = "producer_probe"
                removed_weight = True
        if case == 5 and data.get("kind") in {"group_formed", "allreduce_completed", "averaging_contribution"}:
            data["kind"] = "producer_probe"
    return records


class ArtifactTests(unittest.TestCase):
    def records(self):
        images = {tag: tag + "-id" for tag in study.IMAGES.values()}
        records = [dict(name=f"run-{node}", image_id=images[study.IMAGE], state=dict(ExitCode=0, OOMKilled=False, Running=False))
                   for node in evidence.expected_nodes()]
        records.extend(dict(name="run-" + role, image_id=images[study.IMAGES[key]],
                            state=dict(ExitCode=0, OOMKilled=False, Running=False))
                       for role, key in (("logger", "logger"), ("attack-coordinator", "coordinator")))
        return records, images

    def test_artifacts_require_exact_participants_and_clean_image_matched_exits(self):
        records, images = self.records()
        workflow.validate_container_records(records, "run", images)
        for change in ("duplicate", "oom", "exit", "image", "running"):
            with self.subTest(change=change):
                changed = json.loads(json.dumps(records))
                if change == "duplicate":
                    changed[-1] = changed[0]
                elif change == "image":
                    changed[0]["image_id"] = "unexpected"
                else:
                    changed[0]["state"][{"oom":"OOMKilled", "exit":"ExitCode", "running":"Running"}[change]] = 137 if change == "exit" else True
                with self.assertRaises(ValueError):
                    workflow.validate_container_records(changed, "run", images)

    def test_file_added_during_archival_is_not_silently_omitted(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            source = root / "stage"
            source.mkdir()
            (source / "events.jsonl").write_text("raw")
            verify = workflow.verify_archive
            def add_file(path, metadata):
                verify(path, metadata)
                (source / "late-file").write_text("not archived")
            with patch.object(workflow, "verify_archive", side_effect=add_file):
                with self.assertRaisesRegex(ValueError, "changed during archival"):
                    workflow.archive_run(source, root / "archives", {"run_id":"run"}, {})
            self.assertTrue((source / "events.jsonl").exists())
            self.assertFalse((root / "archives/run.tar.gz").exists())

    def test_storage_error_names_the_volume_and_preserves_reserve(self):
        with patch.object(workflow.shutil, "disk_usage", return_value=Mock(free=7_000_000_000)):
            with self.assertRaisesRegex(ValueError, "local SSD.*7.00 GB.*8 GB"):
                workflow.check_storage(Path("."), Path("."))
        with patch.object(workflow.shutil, "disk_usage", side_effect=[Mock(free=50_000_000_000), Mock(free=19_000_000_000)]):
            with self.assertRaisesRegex(ValueError, "archive volume.*19.00 GB.*20 GB"):
                workflow.check_storage(Path("."), Path("."))


class FullWorkflowTests(unittest.TestCase):
    def test_preparation_eighty_trials_resume_and_report_survive_coverage_and_cleanup_failures(self):
        with tempfile.TemporaryDirectory() as temporary, ExitStack() as patches, redirect_stdout(io.StringIO()):
            root = Path(temporary)
            for name, value in dict(CONFIGS=root/"configs", RESULTS=root/"results", OPERATIONS=root/"operations",
                                    ARCHIVE=root/"archives/raw").items():
                patches.enter_context(patch.object(study, name, value))
            study.ARCHIVE.parent.mkdir(parents=True)
            frozen = dict(images={tag:tag+"-id" for tag in study.IMAGES.values()}, sources=study.sources())
            patches.enter_context(patch.object(workflow, "image_lock", return_value=frozen))
            patches.enter_context(patch.object(workflow, "verify_images"))
            patches.enter_context(patch.object(workflow.shutil, "disk_usage", return_value=Mock(free=100_000_000_000)))
            launched, cases = [], set()

            def simulate(command, log_path, env):
                run_id = command[command.index("--run-id")+1]
                directory = Path(command[command.index("--config-path")+1]).parent
                config = yaml.safe_load((directory/"config.yaml").read_text())
                params = config["node_types"]["victim"]["parameters"]
                cell = f"c{int(params['factor_coordination'])}-f{int(params['factor_filtering'])}-p{int(params['factor_poisoning'])}"
                case = len(launched) % 6
                cases.add(case)
                launched.append(run_id)
                records = cell_records(cell, run_id, case)
                (directory/"events.jsonl").write_text("".join(json.dumps(r)+"\n" for r in records))
                (directory/"events.sqlite3").write_bytes(b"opaque fixture raw database")
                artifacts, images = ArtifactTests().records()
                destination = directory/"container-logs"
                destination.mkdir()
                for artifact in artifacts:
                    artifact["name"] = run_id + artifact["name"][3:]
                    (destination/(artifact["name"]+".json")).write_text(json.dumps(artifact))
                log_path.write_text("Simulated training; no Docker launched.\n")

            patches.enter_context(patch.object(workflow, "run_logged", side_effect=simulate))
            real_remove = shutil.rmtree
            cleanup_failures = []
            def remove(path, *args, **kwargs):
                path = Path(path)
                if path.name.startswith("factorial-main-") and len(cleanup_failures) < 2:
                    cleanup_failures.append(path)
                    if len(cleanup_failures) == 1:
                        raise OSError("simulated local cleanup failure")
                    raise KeyboardInterrupt
                return real_remove(path, *args, **kwargs)
            patches.enter_context(patch.object(workflow.shutil, "rmtree", side_effect=remove))
            workflow.run("pilot", study.ARCHIVE)
            self.assertTrue(json.loads((study.RESULTS/"pilot-readiness.json").read_text())["passed"])
            with self.assertRaises(KeyboardInterrupt):
                workflow.run("main", study.ARCHIVE)
            workflow.run("main", study.ARCHIVE, resume=True)
            workflow.run("main", study.ARCHIVE, resume=True)
            self.assertEqual(len(launched), 88)
            self.assertEqual(cases, set(range(6)))
            self.assertEqual(len(cleanup_failures), 2)
            self.assertTrue(cleanup_failures[0].exists())
            rows = workflow.collapsed_rows(study.RESULTS)
            self.assertEqual(len(rows), 88)
            self.assertTrue(all(r["status"] == "ok" for r in rows))

            write = report.atomic_write
            def fail_audit(path, text):
                if Path(path).name == "analysis-audit.json":
                    raise OSError("simulated report audit failure")
                return write(path, text)
            with patch.object(report, "atomic_write", side_effect=fail_audit):
                with self.assertRaisesRegex(OSError, "report audit failure"):
                    report.export(study.RESULTS, title_prefix="SYNTHETIC TEST — ")
            self.assertFalse((study.RESULTS/"REPORT.md").exists())
            report.export(study.RESULTS, title_prefix="SYNTHETIC TEST — ")
            self.assertTrue((study.RESULTS/"REPORT.md").exists())
            self.assertEqual(len(list((study.RESULTS/"figures").glob("*.png"))), 3)
            self.assertEqual({p.suffix for p in (study.RESULTS / "figures").iterdir()}, {".png"})
            with (study.RESULTS/"data/trials.csv").open() as stream:
                trials = list(csv.DictReader(stream))
            self.assertEqual(len(trials), 80)
            self.assertTrue(any(int(r["successful_groups_with_incomplete_weights"]) for r in trials))
            self.assertTrue(any(int(r["successful_zero_weight_groups"]) for r in trials))
            self.assertTrue(any(int(r["victim_groups"]) == 0 for r in trials))


if __name__ == "__main__":
    unittest.main()
