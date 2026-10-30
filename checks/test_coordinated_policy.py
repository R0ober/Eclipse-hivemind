"""Exercise actual attacker policy; optionally use installed Hivemind offline.

Set DIT616_IMAGE_PROBE=1 in the frozen Linux node image to run these same
admission/assembly/cancellation checks against native Hivemind, without a swarm.
"""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import time
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
NATIVE = os.environ.get("DIT616_IMAGE_PROBE") == "1"


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


store_module = load("coordinated_policy_store", ROOT / "src/eclipse_hivemind/attack_coordinator/store.py")


class FakePeer:
    def __init__(self, raw):
        self.raw = raw

    def __str__(self):
        return self.raw.hex()

    def __hash__(self):
        return hash(self.raw)

    def __eq__(self, other):
        return isinstance(other, FakePeer) and self.raw == other.raw

    @classmethod
    def from_base58(cls, value):
        return cls(bytes.fromhex(value))


class FakeNative:
    @property
    def is_looking_for_group(self):
        return self.lock_looking_for_group.locked()

    def _check_reasons_to_reject(self, request, context):
        if not self.is_looking_for_group or self.assembled_group.done():
            return SimpleNamespace(code="NOT_LOOKING_FOR_GROUP")
        if self.potential_leaders.declared_expiration_time > request.expiration:
            return SimpleNamespace(code="BAD_EXPIRATION_TIME")
        return None

    async def rpc_join_group(self, request, context):
        try:
            async with self.lock_request_join_group:
                rejection = self._check_reasons_to_reject(request, context)
                if rejection is not None:
                    yield rejection
                    return
                self.current_followers[context.remote_id] = request
                yield SimpleNamespace(code="ACCEPTED")
                if len(self.current_followers) == 3:
                    await self.leader_assemble_group()
            group = await asyncio.shield(self.assembled_group)
            yield SimpleNamespace(code="BEGIN_ALLREDUCE", group_id=group.group_id)
        finally:
            self.current_followers.pop(context.remote_id, None)

    async def leader_disband_group(self):
        self.current_followers.clear()

    async def leader_assemble_group(self):
        group = SimpleNamespace(group_id=b"group", peer_ids=(self.peer_id, *self.current_followers))
        self.assembled_group.set_result(group)
        return group


if NATIVE:
    # Verify baked policy/frozen shared modules when the v2 image is used;
    # the original frozen base can instead exercise the mounted new policy.
    sys.path.insert(0, "/app" if Path("/app/coordinated_attacker").is_dir()
                    else str(ROOT / "src/eclipse_hivemind/nodes"))
    from coordinated_attacker import policy as policy_module
    from hivemind.averaging import averager as averager_module
    from hivemind.averaging.matchmaking import Matchmaking as NativeMatchmaking
    from hivemind.averaging.key_manager import GroupKeyManager
    from hivemind.averaging.matchmaking import PotentialLeaders
    from hivemind.p2p import PeerID
    from hivemind.proto import averaging_pb2 as proto
else:
    PeerID = FakePeer
    NativeMatchmaking = FakeNative
    proto = SimpleNamespace(MessageFromLeader=lambda **kwargs: SimpleNamespace(**kwargs),
                            NOT_LOOKING_FOR_GROUP="NOT_LOOKING_FOR_GROUP", ACCEPTED="ACCEPTED",
                            BEGIN_ALLREDUCE="BEGIN_ALLREDUCE", BAD_EXPIRATION_TIME="BAD_EXPIRATION_TIME",
                            MessageCode=SimpleNamespace(Name=lambda code: code))
    averager_module = SimpleNamespace(Matchmaking=NativeMatchmaking)
    def module(name, **values):
        result = ModuleType(name)
        result.__dict__.update(values)
        return result
    replacements = {
        "hivemind": module("hivemind"),
        "hivemind.averaging": module("hivemind.averaging", averager=averager_module),
        "hivemind.p2p": module("hivemind.p2p", PeerID=PeerID),
        "hivemind.proto": module("hivemind.proto", averaging_pb2=proto),
        "hivemind.utils": module("hivemind.utils", get_dht_time=time.time),
        "shared.eclipse_averaging": module("shared.eclipse_averaging", ObservedGradientAverager=object,
                                            install_hooks=lambda *args: None),
    }
    package = module("coordinated_policy_under_test")
    package.__path__ = [str(ROOT / "src/eclipse_hivemind/nodes/coordinated_attacker")]
    replacements[package.__name__] = package
    with patch.dict(sys.modules, replacements):
        policy_module = load(package.__name__ + ".policy",
                             ROOT / "src/eclipse_hivemind/nodes/coordinated_attacker/policy.py")


class Trace:
    def __init__(self):
        self.events = []

    def emit(self, kind, **data):
        self.events.append({"kind": kind, **data})


class Client:
    def __init__(self, store):
        self.store = store

    def claim(self, leader, requester, expiry):
        return self.store.claim("trial", leader, requester, expiry)

    def active_claim(self):
        return self.store.active_claim("trial")

    def finish(self, claim_id, outcome, **details):
        return self.store.finish("trial", claim_id, outcome, **details)


class CoordinatedPolicyTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        directory = tempfile.TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        self.store = store_module.CoordinationStore(Path(directory.name) / "control.sqlite3")
        self.addCleanup(self.store.close)
        self.peers = [PeerID(b"\x12\x20" + bytes([index]) * 32) for index in range(1, 6)]
        self.victim, self.q1, self.q2, self.q3, self.honest = self.peers
        self.store.configure_plan("trial", str(self.victim), [str(peer) for peer in self.peers[1:4]])
        self.trace = Trace()
        plan = policy_module.AttackPlan(str(self.victim), tuple(str(peer) for peer in self.peers[1:4]), "recruiter")
        self.policy = policy_module.CoordinationPolicy(plan, Client(self.store), self.trace)
        patcher = patch.object(averager_module, "Matchmaking", NativeMatchmaking)
        patcher.start()
        self.addCleanup(patcher.stop)
        if NATIVE:
            for cls, name in ((GroupKeyManager, "get_averagers"), (PotentialLeaders, "pop_next_leader"),
                              (PotentialLeaders, "_update_queue_periodically")):
                old = getattr(cls, name)
                self.addCleanup(setattr, cls, name, old)
        roles = {str(peer): {"averager_role": "honest" if peer == self.victim else "controlled"}
                 for peer in self.peers[:4]}
        policy_module.install_coordination_policy(self.trace, roles, "probe.0b", self.policy)
        matcher_class = averager_module.Matchmaking
        self.matcher = matcher_class.__new__(matcher_class)
        m = self.matcher
        m.peer_id, m.schema_hash = self.q1, b"schema"
        m.lock_looking_for_group, m.lock_request_join_group = asyncio.Lock(), asyncio.Lock()
        await m.lock_looking_for_group.acquire()
        m.current_followers, m.current_leader = {}, None
        m.assembled_group = asyncio.get_running_loop().create_future()
        m.was_accepted_to_group, m.follower_was_discarded = asyncio.Event(), asyncio.Event()
        m.target_group_size, m.min_group_size, m.client_mode = 4, 2, False
        async def update(_group):
            pass
        m.group_key_manager = SimpleNamespace(current_key="probe.0b", update_key_on_group_assembled=update)
        m.step_control = SimpleNamespace(data_for_gather=b"ready")
        m.potential_leaders = SimpleNamespace(declared_group_key="probe.0b", declared_expiration_time=time.time() + 10)
        self.streams, self.tasks = [], []

    async def asyncTearDown(self):
        for task in self.tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        for stream in self.streams:
            await stream.aclose()

    def request(self, peer, *, expiry=None):
        values = dict(schema_hash=b"schema", expiration=expiry or time.time() + 20,
                      client_mode=False, gather=b"ready", group_key="probe.0b")
        request = proto.JoinRequest(**values) if NATIVE else SimpleNamespace(**values)
        stream = self.matcher.rpc_join_group(request, SimpleNamespace(remote_id=peer))
        self.streams.append(stream)
        return stream

    async def admit(self, peer):
        stream = self.request(peer)
        message = await anext(stream)
        self.assertEqual(message.code, proto.ACCEPTED)
        task = asyncio.create_task(anext(stream))
        self.tasks.append(task)
        await asyncio.sleep(0)
        return stream, task

    def test_successful_claim_emits_without_duplicate_keywords(self):
        claim = self.policy.claim_for_victim(str(self.q1), time.time() + 20)
        self.assertTrue(claim["accepted"])
        self.assertEqual(self.trace.events[-1]["requested_leader_peer_id"], str(self.q1))

    async def test_unavailable_native_leader_does_not_reserve(self):
        self.matcher.lock_looking_for_group.release()
        message = await anext(self.request(self.victim))
        self.assertEqual(message.code, proto.NOT_LOOKING_FOR_GROUP)
        self.assertIsNone(self.store.active_claim("trial"))
        self.assertTrue(self.store.claim("trial", str(self.q2), str(self.victim), time.time() + 20)["accepted"])

    async def test_native_expiration_rejection_does_not_reserve(self):
        message = await anext(self.request(self.victim, expiry=time.time() + 1))
        self.assertEqual(message.code, proto.BAD_EXPIRATION_TIME)
        self.assertIsNone(self.store.active_claim("trial"))

    async def test_closing_victim_stream_releases_its_claim(self):
        stream = self.request(self.victim)
        self.assertEqual((await anext(stream)).code, proto.ACCEPTED)
        await stream.aclose()
        self.assertIsNone(self.store.active_claim("trial"))
        self.assertFalse(self.matcher.current_followers)

    async def test_cancelled_victim_wait_releases_its_claim(self):
        _, task = await self.admit(self.victim)
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertIsNone(self.store.active_claim("trial"))

    async def test_incomplete_capture_disbands_and_releases_claim(self):
        await self.admit(self.victim)
        async with self.matcher.lock_request_join_group:
            self.assertIsNone(await self.matcher.leader_assemble_group())
        self.assertIsNone(self.store.active_claim("trial"))
        self.assertFalse(self.matcher.current_followers)

    async def test_full_group_completes_claim_and_sends_begin_allreduce(self):
        for peer in (self.victim, self.q2, self.q3):
            await self.admit(peer)
        messages = await asyncio.wait_for(asyncio.gather(*self.tasks), 2)
        self.assertTrue(all(message.code == proto.BEGIN_ALLREDUCE for message in messages))
        self.assertEqual(set(self.matcher.assembled_group.result().peer_ids), set(self.peers[:4]))
        self.assertIsNone(self.store.active_claim("trial"))
        events = [event["kind"] for event in self.trace.events]
        self.assertIn("coordination_claim_completed", events)

    async def test_non_target_is_rejected_without_claim(self):
        message = await anext(self.request(self.honest))
        self.assertEqual(message.code, proto.NOT_LOOKING_FOR_GROUP)
        self.assertIsNone(self.store.active_claim("trial"))

    async def test_old_stream_cleanup_cannot_abort_another_leaders_claim(self):
        stream = self.request(self.victim)
        self.assertEqual((await anext(stream)).code, proto.ACCEPTED)
        original = self.store.active_claim("trial")
        self.store.finish("trial", original["claim_id"], "aborted")
        replacement = self.store.claim("trial", str(self.q2), str(self.victim), time.time() + 20)
        await stream.aclose()
        self.assertEqual(self.store.active_claim("trial")["claim_id"], replacement["claim_id"])

    async def test_support_also_suppresses_victim_leader_requests(self):
        self.policy.plan = replace(self.policy.plan, local_role="support")
        self.assertIsNone(await self.matcher._request_join_group(self.victim))
        self.assertEqual(self.trace.events[-1]["kind"], "victim_leader_request_suppressed")


if __name__ == "__main__":
    unittest.main(verbosity=2)
