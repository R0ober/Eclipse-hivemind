"""Completion acknowledgement must not cause Docker to signal a finishing trainer."""
import importlib.util
import io
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))
from eclipse_hivemind.config import parse_config
from eclipse_hivemind.factorial import study, workflow
from orchestrator import orchestrator
from orchestrator.docker_backend import DockerBackend


class NaturalExitTests(unittest.TestCase):
    def run_fixture(self, directory, backend, wait=True):
        config = parse_config(json.dumps(dict(schema_version=1,
            experiment=dict(name="exit", seed=17, rounds=12),
            logger=dict(endpoint="http://logger:8080"),
            node_types=dict(normal=dict(count=2, image="node:fixture")))))
        def seed(**kwargs):
            kwargs["nodes"].extend(dict(node_id=f"normal-{i}", container_id=f"node-{i}") for i in range(2))
            kwargs["created_containers"].extend(["node-0", "node-1"])
            return {"normal-0": "address"}
        backend.create_network.return_value = "network"
        backend.create_container.return_value = "logger"
        backend.wait_for_log.return_value = "TRAINING_COMPLETE=1"
        with patch.object(orchestrator, "start_seed_nodes", side_effect=seed), \
             patch.object(orchestrator, "_start_node_type"):
            with orchestrator.run(config, backend, run_id="fixture", output_root=Path(directory),
                                  capture_logs=True, wait_for_exit=wait):
                # The body is reachable only after every trainer has exited cleanly.
                backend.body_reached()

    def test_completion_is_followed_by_natural_exit_before_any_cleanup_signal(self):
        backend = Mock()
        backend.wait_for_exit.return_value = 0
        with tempfile.TemporaryDirectory() as directory:
            self.run_fixture(directory, backend)
        names = [call[0] for call in backend.mock_calls]
        self.assertLess(max(i for i, name in enumerate(names) if name == "wait_for_log"), names.index("wait_for_exit"))
        self.assertLess(max(i for i, name in enumerate(names) if name == "wait_for_exit"), names.index("stop"))
        self.assertEqual(backend.wait_for_exit.call_count, 2)
        self.assertTrue(all(0 < call.kwargs["timeout"] <= 120 for call in backend.wait_for_exit.call_args_list))
        backend.body_reached.assert_called_once()

    def test_nonzero_exit_still_fails_and_retains_container_artifacts(self):
        backend = Mock()
        backend.wait_for_exit.side_effect = [0, 137]
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(RuntimeError, "normal-1 exited with code 137"):
                self.run_fixture(directory, backend)
        backend.body_reached.assert_not_called()
        self.assertEqual(backend.capture_artifacts.call_count, 3)

    def test_exit_timeout_still_fails_and_cleans_up_owned_containers(self):
        backend = Mock()
        backend.wait_for_exit.side_effect = TimeoutError("not exited")
        with tempfile.TemporaryDirectory() as directory:
            with self.assertRaisesRegex(TimeoutError, "not exited"):
                self.run_fixture(directory, backend)
        backend.body_reached.assert_not_called()
        self.assertEqual(backend.capture_artifacts.call_count, 3)

    def test_historical_default_does_not_add_exit_waits(self):
        backend = Mock()
        with tempfile.TemporaryDirectory() as directory:
            self.run_fixture(directory, backend, wait=False)
        backend.wait_for_exit.assert_not_called()

    def test_docker_exit_observer_never_sends_stop_or_kill(self):
        backend = object.__new__(DockerBackend)
        backend._client = Mock()
        container = backend._client.containers.get.return_value
        container.wait.return_value = {"StatusCode": 137, "Error": None}
        self.assertEqual(backend.wait_for_exit("trainer", timeout=45), 137)
        container.wait.assert_called_once_with(timeout=45)
        container.stop.assert_not_called()
        container.kill.assert_not_called()


class HostRevisionTests(unittest.TestCase):
    def test_logged_child_shows_heartbeat_and_preserves_stdout_and_stderr(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()) as display:
            path = Path(directory) / "child.log"
            workflow.run_logged([sys.executable, "-c", "import sys,time; print('out'); print('err', file=sys.stderr); time.sleep(.08)"],
                                path, {}, heartbeat_seconds=.01)
            self.assertIn("Still running", display.getvalue())
            self.assertIn("out", path.read_text())
            self.assertIn("err", path.read_text())

    def test_logged_child_failure_is_not_hidden_by_progress_output(self):
        with tempfile.TemporaryDirectory() as directory, redirect_stdout(io.StringIO()):
            with self.assertRaises(subprocess.CalledProcessError):
                workflow.run_logged([sys.executable, "-c", "raise SystemExit(7)"], Path(directory) / "bad.log", {})




if __name__ == "__main__":
    unittest.main()
