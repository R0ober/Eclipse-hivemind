"""Configuration and measurement helpers for the local GroupKey experiment."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass

HIVEMIND_VERSION = "1.1.12"
CONTROLLED_AVERAGER_ROLES = {"controlled", "malicious"}
ACTIVE_AVERAGER_ROLES = {"honest", *CONTROLLED_AVERAGER_ROLES}


def averager_role(behavior: str, *, controlled: bool, override: str | None = None) -> str:
    """Separate attacker ownership from active gradient inversion."""
    if behavior == "disabled":
        return "disabled"
    if override is not None:
        return override
    if controlled:
        return "malicious" if behavior == "reverse-gradient" else "controlled"
    return "honest"


def flag(value: str) -> bool:
    if value.lower() not in {"true", "false", "1", "0"}:
        raise ValueError(f"expected true/false, got {value!r}")
    return value.lower() in {"true", "1"}


@dataclass(frozen=True)
class Settings:
    storage: str
    averager: str
    placement: str
    offset: int
    replicas: int
    run_id: str
    group_key: str
    focal: bool
    storage_poll_seconds: float
    lifetime_seconds: float
    averager_role_override: str | None = None

    @classmethod
    def from_env(cls, *, victim: bool = False) -> Settings:
        storage = os.environ.get("PARAM_STORAGE_BEHAVIOR", "standard" if victim else "filter")
        averager = os.environ.get("PARAM_AVERAGER_BEHAVIOR", "standard" if victim else "disabled")
        placement = os.environ.get("PARAM_DHT_PLACEMENT", "random" if victim else "targeted")
        if storage not in {"standard", "filter"}:
            raise ValueError("storage_behavior must be standard or filter")
        if averager not in {"disabled", "standard", "reverse-gradient"}:
            raise ValueError("averager_behavior must be disabled, standard or reverse-gradient")
        role_override = os.environ.get("PARAM_AVERAGER_ROLE")
        allowed_roles = {"disabled": {"disabled"}, "standard": {"honest", "controlled"},
                         "reverse-gradient": {"malicious"}}
        if role_override is not None and role_override not in allowed_roles[averager]:
            raise ValueError("averager_role must agree with averager_behavior")
        if victim and role_override not in {None, "honest"}:
            raise ValueError("the victim must have honest Averager role")
        if placement not in {"random", "targeted"}:
            raise ValueError("dht_placement must be random or targeted")
        if victim and (storage != "standard" or averager != "standard" or placement != "random"):
            raise ValueError("the victim must use standard storage, standard averaging and random placement")
        # Match the honest runner's default; recompute the target for each launch.
        run_id = os.environ.get("PARAM_AVERAGING_RUN_ID", f"{os.environ['EXPERIMENT_ID']}:toy-classifier:v1")
        if not run_id or "." in run_id:
            raise ValueError("averaging_run_id must be nonempty and contain no dots (Hivemind prefix constraint)")
        offset = int(os.environ.get("PARAM_DHT_ID_OFFSET", "1")) + int(os.environ["NODE_INDEX"])
        replicas = int(os.environ.get("PARAM_NUM_REPLICAS", "5"))
        poll = float(os.environ.get("PARAM_STORAGE_POLL_SECONDS", "1"))
        lifetime = float(os.environ.get("PARAM_STORAGE_LIFETIME_SECONDS", "3500"))
        if not 0 < offset < 2**160 or replicas <= 0:
            raise ValueError("DHT ID offset must be in 1..2**160-1 and num_replicas must be positive")
        if any(not math.isfinite(value) or value <= 0 for value in (poll, lifetime)):
            raise ValueError("storage polling interval and lifetime must be finite and positive")
        focal = flag(os.environ.get("PARAM_FOCAL_VICTIM", str(victim)))
        if focal and not victim:
            raise ValueError("only the standard Victim runner may be designated as the Focal Victim")
        return cls(storage, averager, placement, offset, replicas, run_id,
                   f"{run_id}_grad_averager.0b", focal,
                   poll, lifetime, role_override)


def group_metrics(members: list[str], leader: str | None, roles: dict[str, dict]) -> dict:
    """Unknown roles and absent groups must never become measured zero shares."""
    labels = [roles.get(peer, {}).get("averager_role", "unknown") for peer in members]
    known = all(role in ACTIVE_AVERAGER_ROLES for role in labels)
    malicious = labels.count("malicious") if known and members else None
    controlled = sum(role in CONTROLLED_AVERAGER_ROLES for role in labels) if known and members else None
    return {"members": [{"peer_id": peer, "averager_role": role} for peer, role in zip(members, labels)],
            "group_size": len(members) if members else None,
            "malicious_count": malicious,
            "malicious_share": malicious / len(members) if malicious is not None else None,
            "controlled_count": controlled,
            "controlled_share": controlled / len(members) if controlled is not None else None,
            "leader_peer_id": leader,
            "leader_role": roles.get(leader, {}).get("averager_role", "unknown") if leader else None}
