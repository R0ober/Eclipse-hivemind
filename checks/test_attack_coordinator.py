"""Durability and exclusivity checks for the private coordination channel."""

from __future__ import annotations

from pathlib import Path
import sys
import tempfile
import time
import unittest


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from eclipse_hivemind.attack_coordinator.store import CoordinationStore


class CoordinatorStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.store = CoordinationStore(Path(self.directory.name) / "coordination.sqlite3")
        self.addCleanup(self.store.close)
        self.store.configure_plan("trial", "victim", ["q1", "q2", "q3"])

    def test_plan_is_idempotent_but_cannot_change_peers(self):
        self.assertEqual(self.store.configure_plan("trial", "victim", ["q3", "q2", "q1"])["victim_peer_id"], "victim")
        with self.assertRaisesRegex(ValueError, "different peers"):
            self.store.configure_plan("trial", "other", ["q1", "q2", "q3"])

    def test_only_one_recruiter_can_claim_a_victim_admission(self):
        expiry = time.time() + 20
        first = self.store.claim("trial", "q1", "victim", expiry)
        second = self.store.claim("trial", "q2", "victim", expiry)
        self.assertTrue(first["accepted"])
        self.assertFalse(second["accepted"])
        self.assertEqual(second["reason"], "another_leader_has_active_claim")
        self.assertEqual(self.store.active_claim("trial")["leader_peer_id"], "q1")

    def test_non_victim_requester_is_not_claimable(self):
        result = self.store.claim("trial", "q1", "q2", time.time() + 20)
        self.assertFalse(result["accepted"])
        self.assertEqual(result["reason"], "peer_not_in_plan")

    def test_completed_claim_is_persisted(self):
        claim = self.store.claim("trial", "q1", "victim", time.time() + 20)
        completed = self.store.finish("trial", claim["claim_id"], "completed", group_id="group-1")
        self.assertEqual(completed["status"], "completed")
        self.assertIsNone(self.store.active_claim("trial"))


if __name__ == "__main__":
    unittest.main()
