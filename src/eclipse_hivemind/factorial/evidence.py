"""One streaming evidence definition for pilots, campaign gates and final CSVs."""
from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
import json
import math
from pathlib import Path
import statistics

from .study import ROUNDS, factors


def mean(values):
    values = [v for v in values if v is not None]
    return statistics.fmean(values) if values else None


def ratio(n, d):
    return n / d if d else None


def timestamp(value):
    return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()


def expected_nodes():
    return {f"{role}-{i}" for role, count in (("normal", 14), ("victim", 1), ("recruiter", 3), ("support", 2)) for i in range(count)}


def contribution_coverage(members, weights, recruiters):
    """Describe observed input weights without inferring absent contributions."""
    observed = members & weights.keys()
    complete = weights.keys() == members
    total = sum(weights.values()) if complete else None
    return dict(
        weights_complete=complete,
        weight_total=total,
        observed_weight_total=sum(weights[peer] for peer in observed),
        weight_member_coverage=ratio(len(observed), len(members)),
        missing_weight_peers=json.dumps(sorted(members - weights.keys())),
        zero_weight_group=complete and total == 0,
        recruiter_weight_share=ratio(sum(weights.get(peer, 0) for peer in recruiters), total)
        if complete else None,
    )


def analyse(stream, trial, emit=lambda table, row: None):
    """Never turn empty discovery, no group, failed exchange or unknown into success.

    ``stream`` is an iterable of JSONL bytes/text, including a verified tar member.
    Detail tables are streamed; only small peer/evaluation/group registries stay in RAM.
    """
    c, f, p = factors(trial["cell"])
    common = {k: trial[k] for k in ("cell", "seed", "run_id")}
    started, finished, complete, conditions, transforms = {}, set(), {}, {}, {}
    fingerprints = set()
    groups, reductions, weights, evaluations = {}, defaultdict(set), defaultdict(dict), {}
    success_times = {}
    reasons = set()
    counters = Counter()
    discovery_sum = discovery_n = only_n = empty_n = unknown_n = snapshots = 0
    holder_sum = holder_n = coverage_n = storage_n = 0
    seqs, trace_counts = {}, Counter()
    train_samples, train_steps = Counter(), Counter()
    first_time, last_time = float("inf"), float("-inf")
    for line in stream:
        if not line.strip():
            continue
        record = json.loads(line)
        if record.get("experiment_id") != trial["run_id"]:
            raise ValueError("raw event run identity disagrees with manifest")
        event, node = record["event"], record["node"]["node_id"]
        if node not in expected_nodes():
            reasons.add("unexpected_node")
        data, event_type = event["data"], event["event_type"]
        kind = data.get("kind", event_type)
        t = timestamp(event["occurred_at"])
        first_time, last_time = min(first_time, t), max(last_time, t)
        counters[kind] += 1
        if event_type == "eclipse_observation" and kind != "telemetry_complete":
            trace_counts[node] += 1
            if "trace_sequence" in data:
                key = (node, str(data["process_id"]))
                sequence = data["trace_sequence"]
                if (key in seqs and sequence != seqs[key] + 1) or (key not in seqs and sequence not in (0, 1)):
                    reasons.add("producer_sequence_gap")
                seqs[key] = sequence
            else:
                reasons.add("missing_producer_sequence")
        if event_type == "node_started":
            started[node] = data.get("runtime_metadata", {})
            fingerprints.add(data.get("model_fingerprint"))
            if started[node].get("factorial_runtime_version") != "factorial-v1-20261018":
                reasons.add("wrong_node_runtime_version")
        elif event_type == "node_finished":
            finished.add(node)
        elif kind == "telemetry_complete":
            complete[node] = data
            if data.get("transport_version") != "process_spool_v4" or data.get("acknowledged_observations") != trace_counts[node]:
                reasons.add("incomplete_transport_audit")
            counts = data.get("producer_record_counts", {})
            if len(counts) != data.get("producer_count"):
                reasons.add("producer_count_mismatch")
            for pid, count in counts.items():
                last = seqs.get((node, str(pid)))
                # An idle child still emits its start/end transport markers.
                # The parent has no start marker, only its terminal marker.
                if last is not None:
                    expected_count = last + 2
                else:
                    expected_count = 1 if str(pid) == str(data.get("process_id")) else 2
                if type(count) is not int or expected_count != count:
                    reasons.add("producer_tail_unconfirmed")
        elif kind == "factorial_conditions":
            conditions[node] = data
            if (data.get("coordination"), data.get("filtering"), data.get("poisoning")) != (bool(c), bool(f), bool(p)):
                reasons.add("wrong_factor_delivery")
            expected = "reverse-gradient" if p and node.startswith("recruiter-") else "standard"
            if data.get("gradient_behavior") != expected:
                reasons.add("wrong_gradient_behavior")
        elif kind == "gradient_transform_summary":
            transforms[node] = data
            reverse = bool(p and node.startswith("recruiter-"))
            if data.get("reversed") != reverse or bool(data.get("calls", 0)) != reverse:
                reasons.add("wrong_gradient_transform_delivery")
        elif event_type == "training_metrics":
            train_steps[node] += 1
            train_samples[node] += data.get("samples", 0)
        elif event_type == "eval_metrics":
            key = (node, int(data["round"]))
            value = dict(**common, node_id=node, round=int(data["round"]), time=t,
                         loss=data["eval_loss"], accuracy=data["eval_accuracy"],
                         max_class_fraction=max(data.get("predicted_class_fractions") or [0]),
                         samples=data.get("samples"))
            if data.get("samples") != 4096:
                reasons.add("wrong_evaluation_sample_count")
            if key in evaluations and evaluations[key] != value:
                reasons.add("duplicate_conflicting_evaluation")
            evaluations[key] = value
            emit("evaluations", value)
        elif kind == "candidate_snapshot" and node == "victim-0":
            snapshots += 1
            candidates = {item["peer_id"] for item in data.get("candidates", []) if not item.get("is_self")}
            selected = set(conditions.get(node, {}).get("selected_peer_ids", []))
            registry = {item.get("peer_id") for item in started.values()}
            unknown = bool(candidates - registry)
            share = len(candidates & selected) / len(candidates) if candidates and not unknown and len(selected) == 3 else None
            if share is not None:
                discovery_sum += share
                discovery_n += 1
                only_n += share == 1
            empty_n += not candidates
            unknown_n += unknown
            emit("discovery", dict(**common, time=t, query_id=data.get("query_id"), search_id=data.get("search_id"),
                                    candidates=len(candidates), recruiter_share=share, empty=not candidates, unknown=unknown))
        elif kind == "group_formed":
            members = tuple(sorted(item["peer_id"] for item in data["members"]))
            gid = data["group_id"]
            if gid in groups and (groups[gid]["members"], groups[gid]["leader"]) != (members, data["leader_peer_id"]):
                reasons.add("conflicting_group_membership")
            groups.setdefault(gid, dict(members=members, leader=data["leader_peer_id"], time=data.get("dht_time", t), reporters=set()))
            groups[gid]["reporters"].add(node)
        elif kind == "allreduce_completed":
            reductions[(data["group_id"], node)].add(data.get("success"))
            if data.get("success") is True:
                key = (data["group_id"], node)
                success_times[key] = min(success_times.get(key, t), t)
        elif kind == "averaging_contribution":
            weight = data.get("weight")
            if isinstance(weight, bool) or not isinstance(weight, (int, float)) or not math.isfinite(weight) or weight < 0:
                reasons.add("invalid_averaging_weight")
            else:
                gid, peer = data["group_id"], data["peer_id"]
                if started.get(node, {}).get("peer_id") != peer:
                    reasons.add("contribution_peer_identity_mismatch")
                if peer in weights[gid] and weights[gid][peer] != weight:
                    reasons.add("conflicting_averaging_weight")
                weights[gid][peer] = weight
        elif kind == "storage_control":
            storage_n += 1
            coverage = data.get("complete_coverage") is True
            coverage_n += coverage
            owners = {node for node, info in started.items() if info.get("attacker_controlled")}
            for item in data.get("records", []):
                holders = item.get("holder_nodes", [])
                share = ratio(len(set(holders) & owners), len(holders)) if coverage and item.get("active") is True else None
                if share is not None:
                    holder_sum += share
                    holder_n += 1
                emit("storage", dict(**common, time=data.get("sampling_time", t), complete_coverage=coverage,
                                     peer_id=item.get("peer_id"), holders=len(holders), attacker_holder_share=share))

    expected = expected_nodes()
    for nodes, label in ((set(started), "starts"), (finished, "finishes"), (set(complete), "transport_audits"),
                         (set(conditions), "conditions"), (set(transforms), "transform_summaries")):
        if nodes != expected:
            reasons.add("missing_or_extra_" + label)
    if counters["node_error"] or counters["observation_error"] or counters["telemetry_warning"]:
        reasons.add("raw_telemetry_or_node_error")
    if not f and counters["response_filtered"]:
        reasons.add("unexpected_filtering")
    if not c and any(counters[k] for k in ("victim_leader_request_suppressed", "recruiter_join_suppressed", "coordination_claim_result")):
        reasons.add("unexpected_coordination")
    endpoints = sum((node, ROUNDS) in evaluations for node in expected)
    # A no-group outcome remains valid if the experiment and telemetry completed.
    # Missing learning endpoints are separate coverage, never imputed values.
    peer_to_node = {info.get("peer_id"): node for node, info in started.items()}
    if len(peer_to_node) != 20 or fingerprints == {None} or None in fingerprints or len(fingerprints) != 1:
        reasons.add("wrong_initial_model_or_peer_identities")
    for node, info in started.items():
        owner = node.startswith(("recruiter-", "support-"))
        expected_role = ("malicious" if p else "controlled") if node.startswith("recruiter-") else "honest"
        if info.get("averager_role") != expected_role or info.get("storage_role") != ("malicious" if f and owner else "honest"):
            reasons.add("wrong_runtime_roles")
        if info.get("placement") != ("targeted" if owner else "random"):
            reasons.add("wrong_runtime_placement")
        if info.get("attacker_controlled") is not owner:
            reasons.add("wrong_owner_identity")
    selected = {peer for peer, node in peer_to_node.items() if node.startswith("recruiter-")}
    victim = next((peer for peer, node in peer_to_node.items() if node == "victim-0"), None)
    owner_peers = {peer for peer, node in peer_to_node.items() if started[node].get("attacker_controlled")}
    if len(selected) != 3 or len(owner_peers) != 5 or not victim:
        reasons.add("wrong_fixed_population")
    for data in conditions.values():
        if set(data.get("selected_peer_ids", [])) != selected or data.get("victim_peer_id") != victim:
            reasons.add("wrong_selected_or_victim_identities")
    rows = []
    for gid, group in sorted(groups.items(), key=lambda pair: (pair[1]["time"], pair[0])):
        members = set(group["members"])
        if victim not in members:
            continue
        if "victim-0" not in group["reporters"]:
            counters["unconfirmed_victim_membership_groups"] += 1
            continue
        outcomes = reductions.get((gid, "victim-0"), set())
        outcome = "mixed" if True in outcomes and False in outcomes else "success" if True in outcomes else "failed" if False in outcomes else "unknown"
        if outcome == "mixed":
            reasons.add("conflicting_victim_allreduce")
        if len(members) != len(group["members"]) or not members <= peer_to_node.keys() or group["leader"] not in members:
            reasons.add("invalid_group_membership")
        capture = members == selected | {victim}
        group_weights = weights.get(gid, {})
        if group_weights.keys() - members:
            reasons.add("unexpected_group_contributor")
        # Group formation does not guarantee that every peer starts aggregation.
        # Coverage is a measurement result; transport loss is checked separately.
        coverage = contribution_coverage(members, group_weights, selected)
        row = dict(**common, group_id=gid, time=group["time"], size=len(members),
                   recruiter_partner_share=ratio(len(members & selected), len(members) - 1),
                   recruiter_full_group_share=ratio(len(members & selected), len(members)),
                   owner_partner_share=ratio(len(members & owner_peers), len(members) - 1),
                   recruiter_leader=group["leader"] in selected, owner_leader=group["leader"] in owner_peers,
                   victim_leader=group["leader"] == victim, capture=capture, allreduce=outcome,
                   successful_exchange_time=success_times.get((gid, "victim-0")),
                   **coverage,
                   members=json.dumps(sorted(members)), weights=json.dumps(group_weights, sort_keys=True))
        rows.append(row)
        emit("groups", row)
    captured = [row for row in rows if row["capture"]]
    successes = [row for row in captured if row["allreduce"] == "success"]
    longest = current = longest_successful = current_successful = 0
    for row in rows:
        current = current + 1 if row["capture"] else 0
        longest = max(longest, current)
        current_successful = current_successful + 1 if row["capture"] and row["allreduce"] == "success" else 0
        longest_successful = max(longest_successful, current_successful)
    first_success = min(successes, key=lambda row: row["successful_exchange_time"]) if successes else None
    final = evaluations.get(("victim-0", ROUNDS), {})
    initial = evaluations.get(("victim-0", 0), {})
    honest = [evaluations[(f"normal-{i}", ROUNDS)] for i in range(14) if (f"normal-{i}", ROUNDS) in evaluations]
    result = dict(**common, coordination=c, filtering=f, poisoning=p, technical_valid=not reasons,
                  validity_reasons="|".join(sorted(reasons)), nodes_finished=len(finished),
                  nodes_at_endpoint=endpoints, endpoint_complete=endpoints == 20,
                  victim_accuracy=final.get("accuracy"), victim_loss=final.get("loss"),
                  ordinary_honest_n=len(honest), ordinary_honest_accuracy=mean([r["accuracy"] for r in honest]),
                  ordinary_honest_loss=mean([r["loss"] for r in honest]),
                  worst_ordinary_honest_loss=max((r["loss"] for r in honest), default=None),
                  victim_max_class_fraction=final.get("max_class_fraction"),
                  candidate_share=discovery_sum / discovery_n if discovery_n else None,
                  candidate_snapshots=snapshots, candidate_known_nonempty=discovery_n,
                  empty_candidate_fraction=ratio(empty_n, snapshots), only_recruiter_fraction=ratio(only_n, discovery_n),
                  unknown_candidate_snapshots=unknown_n, victim_groups=len(rows),
                  partner_share=mean([r["recruiter_partner_share"] for r in rows]),
                  recruiter_leader_rate=mean([r["recruiter_leader"] for r in rows]),
                  victim_leader_rate=mean([r["victim_leader"] for r in rows]),
                  full_capture_groups=len(captured), successful_capture_groups=len(successes),
                  full_capture_fraction=ratio(len(captured), len(rows)), successful_capture_fraction=ratio(len(successes), len(rows)),
                  any_successful_capture=bool(successes), longest_capture_sequence=longest,
                  longest_successful_capture_sequence=longest_successful,
                  first_capture_group=(rows.index(first_success) + 1) if first_success else None,
                  first_capture_seconds=(first_success["successful_exchange_time"] - initial["time"]) if first_success and initial else None,
                  first_capture_censored=not successes,
                  observation_seconds=(final["time"] - initial["time"]) if final and initial else None,
                  unknown_allreduces=sum(r["allreduce"] == "unknown" for r in rows),
                  unconfirmed_victim_membership_groups=counters["unconfirmed_victim_membership_groups"],
                  failed_allreduces=sum(r["allreduce"] == "failed" for r in rows),
                  successful_allreduces=sum(r["allreduce"] == "success" for r in rows),
                  victim_steps=train_steps["victim-0"], victim_samples=train_samples["victim-0"],
                  filter_events=counters["response_filtered"], complete_storage_fraction=ratio(coverage_n, storage_n),
                  storage_samples=storage_n, complete_storage_samples=coverage_n,
                  attacker_holder_share=holder_sum / holder_n if holder_n else None,
                  recruiter_weight_share=mean([r["recruiter_weight_share"] for r in rows]),
                  zero_weight_groups=sum(r["zero_weight_group"] for r in rows),
                  successful_zero_weight_groups=sum(r["zero_weight_group"] and r["allreduce"] == "success" for r in rows),
                  groups_with_complete_weights=sum(r["weights_complete"] for r in rows),
                  successful_groups_with_incomplete_weights=sum(not r["weights_complete"] and r["allreduce"] == "success" for r in rows),
                  weight_member_coverage=mean([r["weight_member_coverage"] for r in rows]),
                  groups_with_defined_weight_share=sum(r["recruiter_weight_share"] is not None for r in rows),
                  global_recruiter_fraction=3 / 20, global_owner_fraction=5 / 20,
                  global_gradient_poisoner_fraction=3 / 20 if p else 0.)
    return result


def analyse_path(directory, trial, emit=lambda table, row: None):
    with (Path(directory) / "events.jsonl").open("rb") as stream:
        return analyse(stream, trial, emit)
