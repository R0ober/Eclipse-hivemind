"""Observe the ordinary 1.1.12 matchmaking path in the Averager child process."""

from __future__ import annotations

import asyncio
import contextvars
import functools
import uuid

from hivemind.averaging import averager as averager_module
from hivemind.averaging.key_manager import GroupKeyManager
from hivemind.averaging.matchmaking import Matchmaking, PotentialLeaders
from hivemind.optim.grad_averager import GradientAverager
from hivemind.proto import averaging_pb2
from hivemind.p2p import PeerID
from hivemind.utils import get_dht_time

from .eclipse import ACTIVE_AVERAGER_ROLES, CONTROLLED_AVERAGER_ROLES, group_metrics
from .eclipse_dht import averager_query

search_context = contextvars.ContextVar("eclipse_search", default=None)
attempt_context = contextvars.ContextVar("eclipse_attempt", default=None)


def queue_rows(queue, roles):
    ordered = sorted(queue.items(), key=lambda item: (item[1].expiration_time, item[0].to_bytes()))
    return [{"peer_id": str(peer), "expiration_time": entry.expiration_time, "queue_rank": rank,
             "averager_role": roles.get(str(peer), {}).get("averager_role", "unknown")}
            for rank, (peer, entry) in enumerate(ordered, 1)]


class StreamObserver:
    def __init__(self, stream, trace, attempt):
        self.stream, self.trace, self.attempt = stream, trace, attempt

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            message = await anext(self.stream)
        except asyncio.CancelledError:
            self.attempt["wait_cancelled"] = True
            raise
        except Exception as error:
            self.attempt["rpc_error"] = type(error).__name__
            raise
        code = averaging_pb2.MessageCode.Name(message.code)
        self.attempt["last_code"] = code
        self.attempt["suggested_leader"] = str(PeerID(message.suggested_leader)) if message.suggested_leader else None
        if code in {"GROUP_DISBANDED", "CANCELLED"} and self.attempt["suggested_leader"]:
            self.attempt["redirect_suggested"] = True
            self.trace.emit("redirect_suggested", attempt_id=self.attempt["attempt_id"],
                            search_id=self.attempt["search_id"],
                            suggested_leader_peer_id=self.attempt["suggested_leader"])
        self.trace.emit("join_response", attempt_id=self.attempt["attempt_id"],
                        search_id=self.attempt["search_id"], code=code,
                        suggested_leader_peer_id=self.attempt["suggested_leader"])
        return message

    async def aclose(self):
        return await self.stream.aclose()


class StubObserver:
    def __init__(self, stub, trace):
        self.stub, self.trace = stub, trace

    async def rpc_join_group(self, *args, **kwargs):
        try:
            stream = await self.stub.rpc_join_group(*args, **kwargs)
        except Exception as error:
            attempt_context.get()["rpc_error"] = type(error).__name__
            raise
        return StreamObserver(stream, self.trace, attempt_context.get())


class ServicerObserver:
    def __init__(self, servicer, trace):
        self.servicer, self.trace = servicer, trace

    def get_stub(self, *args, **kwargs):
        return StubObserver(self.servicer.get_stub(*args, **kwargs), self.trace)


def install_hooks(trace, roles, target_key):
    """Install wrappers in a disposable child; no patches leak to honest processes."""
    original_get = GroupKeyManager.get_averagers

    @functools.wraps(original_get)
    async def get_averagers(manager, group_key, only_active):
        if group_key != target_key:
            return await original_get(manager, group_key, only_active)
        context = {"query_id": str(uuid.uuid4()), "search_id": search_context.get(),
                   "query_time": get_dht_time()}
        token = averager_query.set(context)
        manager.eclipse_last_query = context
        try:
            candidates = await original_get(manager, group_key, only_active)
            unique = {str(peer): (peer, expiration) for peer, expiration in candidates}
            partners = {peer: value for peer, value in unique.items() if peer != str(manager.peer_id)}
            labels = [roles.get(peer, {}).get("averager_role", "unknown") for peer in partners]
            trace.emit("candidate_snapshot", **context, group_key=group_key,
                       snapshot_id=context["query_id"], requester_peer_id=str(manager.peer_id),
                       candidates=[{"peer_id": str(peer), "expiration_time": expiration,
                                    "remaining_lifetime": expiration - context["query_time"],
                                    "is_self": peer == manager.peer_id,
                                    "averager_role": roles.get(str(peer), {}).get("averager_role", "unknown")}
                                   for peer, expiration in unique.values()],
                       lookup_candidate_count=len(unique), self_announcement_present=str(manager.peer_id) in unique,
                       candidate_count=len(partners), candidate_denominator="other_averagers_in_lookup",
                       malicious_share=labels.count("malicious") / len(labels) if labels and
                                       all(label in ACTIVE_AVERAGER_ROLES for label in labels) else None,
                       controlled_share=sum(label in CONTROLLED_AVERAGER_ROLES for label in labels) / len(labels)
                                        if labels and all(label in ACTIVE_AVERAGER_ROLES for label in labels) else None,
                       no_honest_candidate="honest" not in labels if
                                           all(label in ACTIVE_AVERAGER_ROLES for label in labels) else None)
            return candidates
        finally:
            averager_query.reset(token)

    GroupKeyManager.get_averagers = get_averagers
    original_declare = GroupKeyManager.declare_averager

    @functools.wraps(original_declare)
    async def declare(manager, group_key, peer_id, expiration_time, looking_for_group=True):
        if group_key == target_key:
            trace.emit("averager_declaration", search_id=search_context.get(), group_key=group_key,
                       peer_id=str(peer_id), expiration_time=expiration_time,
                       looking_for_group=looking_for_group)
        return await original_declare(manager, group_key, peer_id, expiration_time, looking_for_group)

    GroupKeyManager.declare_averager = declare

    original_update_queue = PotentialLeaders._update_queue_periodically
    original_pop = PotentialLeaders.pop_next_leader

    @functools.wraps(original_pop)
    async def pop_next(leaders):
        before = set(leaders.past_attempts)
        peer = await original_pop(leaders)
        newly_attempted = [expiration for candidate, expiration in leaders.past_attempts - before if candidate == peer]
        context = getattr(leaders, "eclipse_last_query", {})
        selected = {"peer_id": str(peer), "expiration_time": newly_attempted[0] if len(newly_attempted) == 1 else None,
                    "queue_rank": next((row["queue_rank"] for row in getattr(leaders, "eclipse_last_queue", [])
                                        if row["peer_id"] == str(peer)), None), **context}
        leaders.eclipse_selected_candidate = selected
        trace.emit("candidate_selected", **selected)
        return peer

    PotentialLeaders.pop_next_leader = pop_next

    class QueueUpdateEvent:
        def __init__(self, event, observe):
            self.event, self.observe = event, observe

        def set(self):
            self.observe()
            return self.event.set()

        def __getattr__(self, name):
            return getattr(self.event, name)

    # Observe at the existing notification point. Execute Hivemind's original
    # queue loop, without copying its filtering, waiting or ordering decisions.
    async def update_queue(leaders, manager):
        def observe():
            context = getattr(manager, "eclipse_last_query", {})
            if manager.current_key == target_key:
                leaders.eclipse_last_query = context
                leaders.eclipse_last_queue = queue_rows(leaders.leader_queue, roles)
                trace.emit("candidate_queue", **context, group_key=manager.current_key,
                           queue=leaders.eclipse_last_queue,
                           declared_expiration_time=leaders.declared_expiration_time,
                           search_end_time=leaders.search_end_time,
                           min_matchmaking_time=leaders.min_matchmaking_time)
        original_event = leaders.update_finished
        leaders.update_finished = QueueUpdateEvent(original_event, observe)
        try:
            return await original_update_queue(leaders, manager)
        finally:
            leaders.update_finished = original_event

    PotentialLeaders._update_queue_periodically = update_queue

    class ObservedMatchmaking(Matchmaking):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._servicer_type = ServicerObserver(self._servicer_type, trace)

        async def look_for_group(self, step):
            search_id = str(uuid.uuid4())
            token = search_context.set(search_id)
            trace.emit("matchmaking_started", search_id=search_id, scheduled_time=step.scheduled_time,
                       deadline=step.deadline, min_matchmaking_time=self.potential_leaders.min_matchmaking_time)
            try:
                group = await super().look_for_group(step)
                trace.emit("matchmaking_completed", search_id=search_id, joined=group is not None,
                           group_id=group.group_id.hex() if group else None,
                           failure_reason=None if group else "no_group_before_deadline")
                return group
            except BaseException as error:
                trace.emit("matchmaking_completed", search_id=search_id, joined=False, group_id=None,
                           failure_reason=type(error).__name__)
                raise
            finally:
                search_context.reset(token)

        async def _request_join_group(self, leader):
            parent = attempt_context.get()
            context = {"attempt_id": str(uuid.uuid4()), "search_id": search_context.get(),
                       "parent_attempt_id": parent["attempt_id"] if parent else None,
                       "candidate_peer_id": str(leader), "redirect_suggested": False, "redirect_attempted": False}
            if parent is not None:
                parent.update(redirect_attempted=True, redirect_attempt_id=context["attempt_id"],
                              redirected_peer_id=str(leader))
                trace.emit("redirect_attempted", attempt_id=parent["attempt_id"],
                           search_id=context["search_id"], redirect_attempt_id=context["attempt_id"],
                           redirected_peer_id=str(leader))
            token = attempt_context.set(context)
            selected = getattr(self.potential_leaders, "eclipse_selected_candidate", {})
            trace.emit("join_attempt", **context,
                       query_id=getattr(self.group_key_manager, "eclipse_last_query", {}).get("query_id"),
                       averager_role=roles.get(str(leader), {}).get("averager_role", "unknown"),
                       candidate_expiration_time=selected.get("expiration_time") if selected.get("peer_id") == str(leader) else None,
                       queue_rank=selected.get("queue_rank") if selected.get("peer_id") == str(leader) else None,
                       request_expiration_time=self.get_request_expiration_time())
            began = get_dht_time()
            try:
                group = await super()._request_join_group(leader)
                outcome = ("redirected" if context.get("redirect_attempted") else
                           "redirect_suggested" if context.get("redirect_suggested") else
                           "accepted" if group else "timeout" if context.get("wait_cancelled") else
                           "rpc_error" if context.get("rpc_error") else "rejected_or_disbanded")
                trace.emit("join_attempt_completed", **context, outcome=outcome,
                           group_id=group.group_id.hex() if group else None, duration=get_dht_time() - began)
                return group
            except BaseException as error:
                trace.emit("join_attempt_completed", **context, outcome=type(error).__name__,
                           group_id=None, duration=get_dht_time() - began)
                raise
            finally:
                attempt_context.reset(token)

        def record_group(self, group, leader):
            trace.emit("group_formed", search_id=search_context.get(), group_id=group.group_id.hex(),
                       **group_metrics([str(peer) for peer in group.peer_ids], str(leader), roles))

        async def leader_assemble_group(self):
            group = await super().leader_assemble_group()
            self.record_group(group, self.peer_id)
            return group

        async def follower_assemble_group(self, leader, message):
            group = await super().follower_assemble_group(leader, message)
            self.record_group(group, leader)
            return group

    averager_module.Matchmaking = ObservedMatchmaking


class ObservedGradientAverager(GradientAverager):
    @classmethod
    def _get_handle_name(cls, namespace, method_name):
        # Class names are part of Hivemind's RPC address. Keep compatibility with
        # unmodified GradientAverager peers despite the instrumentation subclass.
        return GradientAverager._get_handle_name(namespace, method_name)

    def __init__(self, *args, eclipse_trace, eclipse_roles, eclipse_group_key, **kwargs):
        self.eclipse_trace, self.eclipse_roles = eclipse_trace, eclipse_roles
        self.eclipse_group_key = eclipse_group_key
        super().__init__(*args, **kwargs)

    def run(self):
        self.eclipse_trace.producer_started()
        install_hooks(self.eclipse_trace, self.eclipse_roles, self.eclipse_group_key)
        super().run()
        self.eclipse_trace.producer_finished()

    async def _aggregate_with_group(self, group_info, *args, **kwargs):
        try:
            result = await super()._aggregate_with_group(group_info, *args, **kwargs)
            self.eclipse_trace.emit("allreduce_completed", group_id=group_info.group_id.hex(), success=True)
            return result
        except BaseException as error:
            self.eclipse_trace.emit("allreduce_completed", group_id=group_info.group_id.hex(),
                                    success=False, error=type(error).__name__)
            raise
