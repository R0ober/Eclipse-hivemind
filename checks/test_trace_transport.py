"""Real process/connection failures with an outer deadline; no experiment swarm."""
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import unittest

PROBE = Path(__file__).with_name("trace_transport_probe.py")


class TransportChecks(unittest.TestCase):
    def run_probe(self, mode, *arguments, timeout=12):
        process = subprocess.Popen([sys.executable, str(PROBE), "--mode", mode, *arguments],
                                   stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            stdout, stderr = process.communicate(timeout=3)
            return None, stdout, stderr
        return process.returncode, stdout, stderr

    def test_original_shared_queue_deadlocks_after_a_writer_is_killed_mid_frame(self):
        code, stdout, _ = self.run_probe("legacy-corruption", timeout=3)
        self.assertIsNone(code)
        self.assertIn("legacy_writer_terminated", stdout)

    def test_managed_queue_survives_partial_rpc_and_does_not_claim_complete_measurement(self):
        code, stdout, stderr = self.run_probe("managed-corruption")
        self.assertEqual(code, 0, stderr)
        result = json.loads(stdout.splitlines()[-1])
        self.assertEqual(result["measurement_status"], "completed-with-warning")
        self.assertEqual(len(result["missing_producers"]), 1)
        self.assertEqual(result["received"], ["before_kill", "after_kill", "telemetry_warning"])
        self.assertLess(result["seconds_to_close"], 3)
        self.assertFalse(result["helper_alive"])

    def test_unacknowledged_warning_is_a_delivery_failure(self):
        code, stdout, stderr = self.run_probe("managed-warning-failure")
        self.assertEqual(code, 0, stderr)
        result = json.loads(stdout.splitlines()[-1])
        self.assertEqual(result["measurement_status"], "failed")
        self.assertIn("failed delivery", result["reason"])
        self.assertFalse(result["helper_alive"])

    def test_live_producer_is_neither_terminated_nor_reported_complete(self):
        code, stdout, stderr = self.run_probe("live-producer")
        self.assertEqual(code, 0, stderr)
        result = json.loads(stdout.splitlines()[-1])
        self.assertEqual(result["measurement_status"], "failed")
        self.assertIn("still running", result["reason"])
        self.assertTrue(result["producer_alive"])
        self.assertFalse(result["helper_alive"])

    def test_normal_multi_process_delivery_preserves_all_records_and_per_producer_order(self):
        code, stdout, stderr = self.run_probe("benchmark", "--count", "250", "--payload-bytes", "8192")
        self.assertEqual(code, 0, stderr)
        result = json.loads(stdout.splitlines()[-1])
        self.assertEqual(result["observations"], 500)


if __name__ == "__main__": unittest.main()
