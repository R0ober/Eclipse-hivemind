"""Attacker-only matchmaking rules for the coordinated capture attempt."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass

from hivemind.averaging import averager as averager_module
from hivemind.p2p import PeerID
from hivemind.proto import averaging_pb2
from hivemind.utils import get_dht_time

from shared.eclipse_averaging import ObservedGradientAverager, install_hooks

from .coordinator import CoordinatorClient


@dataclass(frozen=True)
class AttackPlan:
    victim_peer_id: str
    recruiter_peer_ids: tuple[str, str, str]
    local_role: str


class CoordinationPolicy:
    """Private control decisions, kept separate from the observation hooks."""

    def __init__(self, plan: AttackPlan, client: CoordinatorClient, trace) -> None:
        self.plan = plan
        self.client = client
        self.trace = trace

    @property
    def is_recruiter(self) -> bool:
        return self.plan.local_role == "recruiter"

    def emit(self, kind: str, **data) -> None:
        self.trace.emit(kind, local_coordination_role=self.plan.local_role, **data)

    def active_claim(self) -> dict | None:
        try:
            return self.client.active_claim()
        except RuntimeError as error:
            self.emit("coordination_error", operation="active_claim", error=str(error))
            return None

    def claim_for_victim(self, leader_peer_id: str, expiration: float) -> dict | None:
        try:
            claim = self.client.claim(leader_peer_id, self.plan.victim_peer_id, expiration)
        except RuntimeError as error:
            self.emit("coordination_error", operation="claim", error=str(error))
            return None
        self.emit("coordination_claim_result", requested_leader_peer_id=leader_peer_id, **claim)
        return claim

    def finish(self, claim_id: str, outcome: str, **details) -> None:
        try:
            result = self.client.finish(claim_id, outcome, **details)
            self.emit(f"coordination_claim_{outcome}", **result, **details)
        except RuntimeError as error:
            self.emit("coordination_error", operation=f"finish_{outcome}", error=str(error), claim_id=claim_id)


def install_coordination_policy(trace, roles, group_key: str, policy: CoordinationPolicy) -> None:
    """Add the coordinated attacker behavior after installing passive tracing.

    This executes only in an attacker Averager child process. Standard Victim
    and normal-node processes use their existing Hivemind Matchmaking class.
    """
    install_hooks(trace, roles, group_key)
    observed_base = averager_module.Matchmaking

    class CoordinatedMatchmaking(observed_base):
        def _finish_local_claim(self, outcome: str, **details) -> None:
            # Keep the exact reservation owned by this admission. Looking up the
            # service's current claim could finish a later, unrelated attempt.
            claim_id = getattr(self, "_coordination_claim_id", None)
            if claim_id is not None:
                policy.finish(claim_id, outcome, **details)
                self._coordination_claim_id = None
                self._coordination_victim_request = None

        def _check_reasons_to_reject(self, request, context):
            # Hivemind invokes this synchronously under lock_request_join_group.
            # Reserve only after its readiness/schema/key/expiration checks pass.
            rejection = super()._check_reasons_to_reject(request, context)
            if rejection is not None or not policy.is_recruiter:
                return rejection
            requester = str(context.remote_id)
            local_peer_id = str(self.peer_id)
            if requester == policy.plan.victim_peer_id:
                claim = policy.claim_for_victim(local_peer_id, float(request.expiration))
                if claim is not None and claim.get("accepted") and claim.get("leader_peer_id") == local_peer_id:
                    self._coordination_claim_id = claim["claim_id"]
                    self._coordination_victim_request = request
                    policy.emit("victim_admission_claim_authorized", requester_peer_id=requester,
                                claim_id=claim["claim_id"])
                    return None
                policy.emit("victim_admission_claim_rejected", requester_peer_id=requester,
                            reason=(claim or {}).get("reason", "coordinator_unavailable"))
            elif requester in policy.plan.recruiter_peer_ids:
                claim = policy.active_claim()
                if (claim is not None and claim["leader_peer_id"] == local_peer_id
                        and claim["claim_id"] == getattr(self, "_coordination_claim_id", None)
                        and any(str(peer) == policy.plan.victim_peer_id for peer in self.current_followers)):
                    return None
                policy.emit("recruiter_admission_rejected", requester_peer_id=requester,
                            reason="no_matching_victim_admission")
            else:
                policy.emit("non_target_admission_rejected", requester_peer_id=requester)
            return averaging_pb2.MessageFromLeader(code=averaging_pb2.NOT_LOOKING_FOR_GROUP)

        def _members_complete_capture(self) -> bool:
            members = {str(self.peer_id), *(str(peer) for peer in self.current_followers)}
            return members == {policy.plan.victim_peer_id, *policy.plan.recruiter_peer_ids}

        async def _request_join_group(self, leader):
            leader_peer_id = str(leader)
            if leader_peer_id == policy.plan.victim_peer_id:
                policy.emit("victim_leader_request_suppressed", target_peer_id=leader_peer_id)
                return None
            if policy.is_recruiter:
                claim = policy.active_claim()
                if claim is None or claim["leader_peer_id"] != leader_peer_id or leader_peer_id == str(self.peer_id):
                    policy.emit("recruiter_join_suppressed", target_peer_id=leader_peer_id,
                                reason="no_matching_victim_claim")
                    return None
            return await super()._request_join_group(leader)

        async def _request_join_potential_leaders(self, step):
            if not policy.is_recruiter:
                return await super()._request_join_potential_leaders(step)
            assert self.is_looking_for_group
            try:
                async with self.potential_leaders.begin_search(step, self.group_key_manager, declare=not self.client_mode):
                    while get_dht_time() < self.potential_leaders.search_end_time:
                        if self.assembled_group.done():
                            return self.assembled_group.result()
                        claim = policy.active_claim()
                        if claim is not None and claim["leader_peer_id"] != str(self.peer_id):
                            group = await self._request_join_group(PeerID.from_base58(claim["leader_peer_id"]))
                            if group is not None:
                                return group
                        await asyncio.sleep(0.05)
                    return None
            except Exception as error:
                if not self.assembled_group.done():
                    self.assembled_group.set_exception(error)
                raise
            finally:
                if getattr(self, "_coordination_claim_id", None) is not None:
                    async with self.lock_request_join_group:
                        await self.leader_disband_group()

        async def rpc_join_group(self, request, context):
            requester_peer_id = str(context.remote_id)
            stream = super().rpc_join_group(request, context)
            try:
                async for message in stream:
                    policy.emit("attacker_admission_response", requester_peer_id=requester_peer_id,
                                response_code=averaging_pb2.MessageCode.Name(message.code))
                    yield message
            finally:
                # Explicitly close the native generator first, releasing its
                # lock and removing this follower even on cancellation/aclose.
                await stream.aclose()
                if getattr(self, "_coordination_victim_request", None) is request:
                    async with self.lock_request_join_group:
                        await self.leader_disband_group()

        async def leader_assemble_group(self):
            if policy.is_recruiter and not self._members_complete_capture():
                policy.emit("incomplete_capture_disbanded", members=[str(self.peer_id),
                                                                        *(str(peer) for peer in self.current_followers)])
                await self.leader_disband_group()
                return None
            group = await super().leader_assemble_group()
            if policy.is_recruiter:
                self._finish_local_claim("completed", group_id=group.group_id.hex())
            return group

        async def leader_disband_group(self):
            if policy.is_recruiter:
                self._finish_local_claim("aborted", reason="group_disbanded")
            return await super().leader_disband_group()

    averager_module.Matchmaking = CoordinatedMatchmaking


class CoordinatedGradientAverager(ObservedGradientAverager):
    def __init__(self, *args, coordination_policy: CoordinationPolicy, **kwargs):
        self.coordination_policy = coordination_policy
        super().__init__(*args, **kwargs)

    def run(self):
        self.eclipse_trace.producer_started()
        install_coordination_policy(self.eclipse_trace, self.eclipse_roles, self.eclipse_group_key,
                                    self.coordination_policy)
        try:
            super(ObservedGradientAverager, self).run()
        finally:
            self.eclipse_trace.producer_finished()
