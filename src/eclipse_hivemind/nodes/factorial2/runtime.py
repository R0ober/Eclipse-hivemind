"""Independent treatment switches with unchanged native honest matchmaking."""
from __future__ import annotations

from dataclasses import asdict
import os

import honest_runner
from hivemind.dht.routing import DHTID
from hivemind.optim.grad_averager import GradientAverager
from shared.dht import start_dht
from shared.eclipse import Settings, flag
from shared.eclipse_dht import ObservedDHT, metadata
from shared.eclipse_averaging import ObservedGradientAverager
from shared.eclipse_runtime import Runtime
from coordinated_attacker.coordinator import CoordinatorClient
from coordinated_attacker.policy import AttackPlan, CoordinatedGradientAverager, CoordinationPolicy
from .trace import Trace

DHT_SHUTDOWN_TIMEOUT_SECONDS = 30


class AuditedDHT(ObservedDHT):
    def run(self):
        try:
            return super().run()
        finally:
            self.eclipse_trace.producer_finished()


class ContributionAudit:
    async def _aggregate_with_group(self, group_info, *args, **kwargs):
        # This is the actual weight passed by the native optimizer, not an
        # override, inferred sample count or member-count proxy.
        self.eclipse_trace.emit("averaging_contribution", group_id=group_info.group_id.hex(),
                                peer_id=str(self.peer_id), weight=kwargs.get("weight"))
        return await super()._aggregate_with_group(group_info, *args, **kwargs)


class AuditedAverager(ContributionAudit, ObservedGradientAverager):
    def run(self):
        try:
            return super().run()
        finally:
            self.eclipse_trace.producer_finished()


class ContributionOnlyAverager(ContributionAudit, GradientAverager):
    """Ordinary honest peers retain native discovery; only log contribution weights."""
    @classmethod
    def _get_handle_name(cls, namespace, method_name):
        return GradientAverager._get_handle_name(namespace, method_name)

    def __init__(self, *args, eclipse_trace, eclipse_roles, eclipse_group_key, **kwargs):
        self.eclipse_trace = eclipse_trace
        super().__init__(*args, **kwargs)

    def run(self):
        self.eclipse_trace.producer_started()
        try:
            return super().run()
        finally:
            self.eclipse_trace.producer_finished()


class AuditedCoordinatedAverager(ContributionAudit, CoordinatedGradientAverager):
    pass


class FactorialRuntime(Runtime):
    def __init__(self, settings, role, coordination):
        self.role, self.coordination = role, coordination
        self.owner = role in {"recruiter", "support"}
        self.transform_calls = 0
        super().__init__(settings, malicious=self.owner, trace_factory=Trace)

    def start_dht(self):
        # Native IPC receive-error recovery can outlast its three-second
        # shutdown grace. Allow the DHT to consume the queued shutdown command.
        bucket_size = int(os.environ["PARAM_BUCKET_SIZE"])
        if bucket_size <= 0:
            raise ValueError("bucket_size must be a positive integer")
        options = {"num_replicas": self.settings.replicas, "bucket_size": bucket_size,
                   "shutdown_timeout": DHT_SHUTDOWN_TIMEOUT_SECONDS}
        if self.settings.placement == "targeted":
            options["node_id"] = DHTID(int(DHTID.generate(source=self.settings.group_key)) ^ self.settings.offset)
        dht = start_dht(dht_class=AuditedDHT, eclipse_settings=self.settings, eclipse_trace=self.trace,
                        malicious_averager=self.owner, **options)
        self.trace.register_process(dht)
        try:
            dht.eclipse_metadata = dht.run_coroutine(metadata)
        except BaseException:
            dht.shutdown()
            raise
        dht.eclipse_metadata.update(attacker_controlled=self.owner,
                                   coordination_role=self.role if self.owner else None,
                                   coordination_enabled=self.coordination and self.owner,
                                   victim_leader_requests_suppressed=self.coordination and self.owner,
                                   contribution_weight_observed=True,
                                   factorial_runtime_version="factorial-v1-20261018")
        return dht

    def after_barrier(self, dht):
        super().after_barrier(dht)
        self.policy = None
        recruiters = sorted(peer for peer, info in self.roles.items() if info.get("coordination_role") == "recruiter")
        victims = [peer for peer, info in self.roles.items() if info.get("focal_victim")]
        if len(recruiters) != 3 or len(victims) != 1 or len(self.roles) != 20:
            raise RuntimeError("factorial study requires 20 nodes, one Victim, three recruiters")
        if self.owner and self.coordination:
            client = CoordinatorClient(os.environ["PARAM_COORDINATOR_ENDPOINT"], self.identity["experiment_id"])
            client.configure_plan(victims[0], recruiters)
            self.policy = CoordinationPolicy(AttackPlan(victims[0], tuple(recruiters), self.role), client, self.trace)
        self.trace.emit("factorial_conditions", role=self.role, coordination=self.coordination,
                        filtering=flag(os.environ["PARAM_FACTOR_FILTERING"]),
                        poisoning=flag(os.environ["PARAM_FACTOR_POISONING"]),
                        gradient_behavior=self.settings.averager, transport_version=Trace.TRANSPORT_VERSION,
                        selected_peer_ids=recruiters, victim_peer_id=victims[0])

    def averager_factory(self, *args, **kwargs):
        options = dict(eclipse_trace=self.trace, eclipse_roles=self.roles, eclipse_group_key=self.settings.group_key)
        cls = ContributionOnlyAverager if self.role == "normal" else AuditedAverager
        if self.policy is not None:
            cls = AuditedCoordinatedAverager
            options["coordination_policy"] = self.policy
        return self.trace.register_process(cls(*args, **options, **kwargs))

    def reverse_gradients(self, model):
        for parameter in model.parameters():
            if parameter.grad is not None:
                parameter.grad.neg_()
        self.transform_calls += 1


def main():
    role = os.environ["PARAM_FACTORIAL_ROLE"]
    if role not in {"normal", "victim", "recruiter", "support"}:
        raise ValueError("invalid factorial role")
    coordination = flag(os.environ["PARAM_FACTOR_COORDINATION"])
    poison = flag(os.environ["PARAM_FACTOR_POISONING"])
    settings = Settings.from_env(victim=role in {"normal", "victim"})
    reverse = poison and role == "recruiter"
    if (settings.averager == "reverse-gradient") != reverse:
        raise ValueError("gradient behavior disagrees with poisoning factor/role")
    if (settings.storage == "filter") != (flag(os.environ["PARAM_FACTOR_FILTERING"]) and role in {"recruiter", "support"}):
        raise ValueError("storage behavior disagrees with filtering factor/role")
    runtime = FactorialRuntime(settings, role, coordination)
    try:
        honest_runner.main(dht_factory=runtime.start_dht, after_started=runtime.started,
                           after_barrier=runtime.after_barrier, averaging_run_id=settings.run_id,
                           averager_factory=runtime.averager_factory,
                           gradient_transform=runtime.reverse_gradients if reverse else None,
                           extra_settings={"eclipse": asdict(settings), "factorial_role": role},
                           strict_barrier=True, report_complete=False)
    finally:
        runtime.trace.emit("gradient_transform_summary", reversed=reverse, calls=runtime.transform_calls)
        runtime.close()
    runtime.report_finished()
