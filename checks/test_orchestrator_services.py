"""Service containers must stay outside the Hivemind participant manifest."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import Mock


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from eclipse_hivemind.config import ExperimentConfig
from orchestrator.orchestrator import build_experiment_manifest, start_services


class ServiceTests(unittest.TestCase):
    def config(self):
        return ExperimentConfig.model_validate({
            "schema_version": 1,
            "experiment": {"name": "services", "seed": 17, "rounds": 1},
            "logger": {"endpoint": "http://logger:8080"},
            "services": {"attack-coordinator": {"image": "coordinator:v1", "ready_log": "READY",
                                                    "environment": {"COORDINATION_DB_PATH": "/outputs/a.sqlite3"}}},
            "node_types": {"normal": {"count": 1, "image": "node:v1"}},
        })

    def test_manifest_contains_only_participants(self):
        manifest = build_experiment_manifest(self.config(), "run")
        self.assertIn('"node_id":"normal-0"', manifest)
        self.assertNotIn("attack-coordinator", manifest)

    def test_service_receives_output_mount_and_network_alias(self):
        backend = Mock()
        backend.create_container.return_value = "service-id"
        created = []
        with tempfile.TemporaryDirectory() as directory:
            start_services(self.config(), backend, "run", "network", {"eclipse_run": "run"}, created,
                           Path(directory))
        self.assertEqual(created, ["service-id"])
        call = backend.create_container.call_args.kwargs
        self.assertEqual(call["network_aliases"], ["attack-coordinator"])
        self.assertEqual(call["env"]["EXPERIMENT_ID"], "run")
        self.assertEqual(call["env"]["COORDINATION_DB_PATH"], "/outputs/a.sqlite3")
        backend.wait_for_log.assert_called_once_with("service-id", "READY", 60)


if __name__ == "__main__":
    unittest.main()
