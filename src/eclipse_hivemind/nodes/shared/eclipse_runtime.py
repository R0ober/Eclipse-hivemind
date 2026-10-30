"""Lifecycle plumbing shared by the two GroupKey runners."""

from __future__ import annotations

import functools
import multiprocessing
import os
import threading

from hivemind.dht.routing import DHTID

from . import client
from .dht import start_dht
from .eclipse import ACTIVE_AVERAGER_ROLES, CONTROLLED_AVERAGER_ROLES, Settings
from .eclipse_averaging import ObservedGradientAverager
from .eclipse_dht import ObservedDHT, configure_roles, metadata, verify_version
from .eclipse_trace import Trace


class Runtime:
    def __init__(self, settings: Settings, *, malicious: bool, trace_factory=None):
        verify_version()
        if multiprocessing.get_start_method() != "fork":
            raise RuntimeError("these 1.1.12 hooks require fork; use the supplied local Docker images")
        self.settings, self.malicious = settings, malicious
        self.identity = {"experiment_id": os.environ["EXPERIMENT_ID"], "node_id": os.environ["NODE_ID"],
                         "node_type": os.environ["NODE_TYPE"], "node_index": int(os.environ["NODE_INDEX"])}
        self.endpoint = client.logger_endpoint_from_env()
        self.trace = (trace_factory or Trace)(self.identity, self.endpoint)
        self.roles = {}
        self.stop = threading.Event()
        self.monitor = None
        self.closed = False
        self.telemetry_warning = False

    def start_dht(self):
        options = {"num_replicas": self.settings.replicas}
        if self.settings.placement == "targeted":
            target = DHTID.generate(source=self.settings.group_key)
            options["node_id"] = DHTID(int(target) ^ self.settings.offset)
        dht = start_dht(dht_class=ObservedDHT, eclipse_settings=self.settings, eclipse_trace=self.trace,
                        malicious_averager=self.malicious and self.settings.averager != "disabled", **options)
        try:
            dht.eclipse_metadata = dht.run_coroutine(metadata)
        except BaseException:
            dht.shutdown()
            raise
        return self.trace.register_process(dht)

    def started(self, dht):
        self.trace.start()

    def state(self):
        return client.get_runtime(logger_endpoint=self.endpoint, experiment_id=self.identity["experiment_id"])

    def observe_after_barrier(self, dht):
        """Observation failures must not change the standard honest startup policy."""
        try:
            self.after_barrier(dht)
        except Exception as error:
            self.trace.emit("observation_error", source="startup_roles", error=str(error))

    def after_barrier(self, dht):
        state = self.state()
        for node in state["nodes"]:
            info = node["runtime_metadata"]
            if not node["started"] or info is None or not info.get("peer_id"):
                raise RuntimeError(f"missing startup PeerID for {node['node_id']}; use one startup cohort")
            if info.get("averager_role") not in {*ACTIVE_AVERAGER_ROLES, "disabled"} or info.get("storage_role") not in {"honest", "malicious"}:
                raise RuntimeError(f"declare storage_role and averager_role in the config for standard peer {node['node_id']}")
            if info["group_key"] is not None and info["group_key"] != self.settings.group_key:
                raise RuntimeError("all nodes must use the same averaging_run_id and gradient GroupKey")
            if info["group_key"] is None and self.settings.run_id != f"{self.identity['experiment_id']}:toy-classifier:v1":
                raise RuntimeError("standard runners use the launch run ID; omit averaging_run_id in this condition")
            if info["num_replicas"] is not None and info["num_replicas"] != self.settings.replicas:
                raise RuntimeError("all nodes must use the same num_replicas")
            if info["num_replicas"] is None and self.settings.replicas != 5:
                raise RuntimeError("standard 1.1.12 runners use num_replicas=5; use that setting in this condition")
            if info["peer_id"] in self.roles:
                raise RuntimeError("duplicate PeerID in experiment")
            self.roles[info["peer_id"]] = {**info, "node_id": node["node_id"]}
        if sum(info["focal_victim"] for info in self.roles.values()) != 1:
            raise RuntimeError("configure exactly one Focal Victim per trial")
        dht_ids = [info["dht_id"] for info in self.roles.values() if info["dht_id"] is not None]
        if len(set(dht_ids)) != len(dht_ids):
            raise RuntimeError("duplicate DHTID; give each targeted node type a disjoint dht_id_offset range")
        dht.run_coroutine(functools.partial(configure_roles, roles=self.roles))
        self.trace.emit("trial_conditions", averaging_run_id=self.settings.run_id,
                        experiment_seed=int(os.environ["EXPERIMENT_SEED"]), node_seed=int(os.environ["NODE_SEED"]),
                        group_key=self.settings.group_key, configured_r=self.settings.replicas,
                        m=sum(info["storage_role"] == "malicious" and info["placement"] == "targeted"
                              for info in self.roles.values()),
                        malicious_storage_count=sum(info["storage_role"] == "malicious" for info in self.roles.values()),
                        targeted_node_count=sum(info["placement"] == "targeted" for info in self.roles.values()),
                        malicious_averager_count=sum(info["averager_role"] == "malicious" for info in self.roles.values()),
                        controlled_averager_count=sum(info["averager_role"] in CONTROLLED_AVERAGER_ROLES
                                                     for info in self.roles.values()),
                        placement_ranks=[{"peer_id": peer, "rank": rank, **info}
                                         for rank, (peer, info) in enumerate(sorted(
                                              ((peer, info) for peer, info in self.roles.items() if info["xor_distance"] is not None),
                                              key=lambda pair: pair[1]["xor_distance"]), 1)],
                        placement_rank_scope="instrumented_nodes_only",
                        roles=self.roles,
                        expiration_policy="hivemind_standard", admission_policy="hivemind_standard",
                        filtering_scope="all_requesters", filtering_point="remote_find_response")
        if self.settings.focal:
            self.monitor = threading.Thread(target=self._observe_control, daemon=True)
            self.monitor.start()

    def _observe_control(self):
        while not self.stop.is_set():
            try:
                control = self.state()["storage_control"].get(self.settings.group_key)
                if control is not None:
                    self.trace.emit("storage_control", group_key=self.settings.group_key, **control)
            except Exception as error:
                self.trace.emit("observation_error", source="storage_control", error=str(error))
            self.stop.wait(self.settings.storage_poll_seconds)

    def averager_factory(self, *args, **kwargs):
        return self.trace.register_process(ObservedGradientAverager(
            *args, eclipse_trace=self.trace, eclipse_roles=self.roles,
            eclipse_group_key=self.settings.group_key, **kwargs))

    def close(self):
        self.stop.set()
        if self.monitor is not None:
            self.monitor.join(timeout=10)
            if self.monitor.is_alive():
                raise RuntimeError("storage-control observer did not stop; telemetry completion is unconfirmed")
        self.telemetry_warning = bool(self.trace.close())
        self.closed = True

    def report_finished(self):
        """Call only after the DHT/averager producers stop and close succeeds."""
        if not self.closed:
            raise RuntimeError("cannot report completion before telemetry closes successfully")
        client.send_node_finished(logger_endpoint=self.endpoint, **self.identity)
        warning = " ECLIPSE_TELEMETRY_WARNING=producer_end_unconfirmed" if self.telemetry_warning else ""
        print("TRAINING_COMPLETE=1" + warning, flush=True)
