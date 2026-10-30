"""Check the publication against recorded measurements and scientific inputs."""
import csv
import hashlib
import importlib
import json
from pathlib import Path
import sys
import unittest

import yaml

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


class PublicationTests(unittest.TestCase):
    def test_measured_data_checksums_and_balanced_cohorts(self):
        provenance = json.loads((ROOT / "data/provenance.json").read_text())
        for name, record in provenance.items():
            for filename, expected in record["data_sha256"].items():
                self.assertEqual(hashlib.sha256((ROOT / "data" / name / filename).read_bytes()).hexdigest(), expected)
            with (ROOT / "data" / name / "trials.csv").open() as stream:
                rows = list(csv.DictReader(stream))
            self.assertEqual(len(rows), 80)
            self.assertEqual(len({(r["cell"], r["seed"]) for r in rows}), 80)
            self.assertEqual({int(r["seed"]) for r in rows}, set(range(17, 27)))
            self.assertEqual(len(record["archives"]), 80)
            self.assertEqual(len(record["technical_exclusions"]), 0 if name == "sc1" else 5)

    def test_runtime_checksums(self):
        for filename, expected in json.loads((ROOT / "data/runtime-sha256.json").read_text()).items():
            self.assertEqual(hashlib.sha256((ROOT / filename).read_bytes()).hexdigest(), expected, filename)

    def test_all_scientific_configs_match_original_measurements(self):
        original = json.loads((ROOT / "data/config-sha256.json").read_text())
        for name, package in (("sc1", "factorial"), ("sc2", "factorial2")):
            study = importlib.import_module(f"eclipse_hivemind.{package}.study")
            self.assertEqual(len(study.plan("main")), 80)
            self.assertEqual(len(study.plan("pilot")), 8)
            for row in study.plan("main") + study.plan("pilot"):
                path = Path(row["config"])
                generated = study.config(row["cell"], row["seed"])
                self.assertEqual(yaml.safe_load(path.read_text()), generated)
                self.assertEqual(study.sha(path), row["config_sha256"])
                generated["logger"]["image"] = "<logger>"
                for service in generated["services"].values():
                    service["image"] = "<service>"
                for node in generated["node_types"].values():
                    node["image"] = "<node>"
                digest = hashlib.sha256(json.dumps(generated, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
                self.assertEqual(digest, original[name][str(path.relative_to(study.CONFIGS))])

    def test_manifest_input_lock_rejects_changed_configuration(self):
        for package in ("factorial", "factorial2"):
            workflow = importlib.import_module(f"eclipse_hivemind.{package}.workflow")
            lock = dict(sources={"source": "sha"}, images={"image": "id"})
            with self.assertRaises(ValueError):
                workflow.verify_trial_inputs(dict(lock, config_sha256="changed"),
                                             {"image_lock": lock}, {"config_sha256": "original"})

    def test_sc2_archive_cannot_overlap_published_data(self):
        from unittest.mock import patch
        from eclipse_hivemind.factorial2 import study, safety
        with patch.object(study, "ARCHIVE", ROOT / "data/sc2/raw"):
            with self.assertRaises(ValueError):
                safety.validate_layout()


if __name__ == "__main__":
    unittest.main()
