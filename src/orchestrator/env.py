"""
Pure construction of a node's container environment. No Docker here on purpose:
this is the most logic-dense, easiest-to-subtly-break part, so it's a plain
function you can unit-test on its own. This is the exact contract runner.py reads.
"""

from __future__ import annotations

import hashlib

from eclipse_hivemind.config import ExperimentConfig


def derive_node_seed(experiment_seed: int, node_id: str) -> int:
    """Per-node seed derived from the single experiment seed see ADR 0005.
    Can be changed to something else if we want 
    """
    digest = hashlib.sha256(f"{experiment_seed}:{node_id}".encode()).hexdigest()
    return int(digest[:16], 16)


def param_env(parameters: dict) -> dict[str, str]:
    """{"target_prefix": "expert."} -> {"PARAM_TARGET_PREFIX": "expert."}"""
    return {f"PARAM_{key.upper()}": str(value) for key, value in parameters.items()}


def build_node_env(
    config: ExperimentConfig,
    run_id: str,
    node_type_name: str,
    node_index: int,
    initial_peers: list[str],
    identity_path: str | None = None,
) -> dict[str, str]:
    node_id = f"{node_type_name}-{node_index}"
    node_type = config.node_types[node_type_name]

    env = {
        "EXPERIMENT_ID": run_id,
        "EXPERIMENT_NAME": config.experiment.name,
        "EXPERIMENT_SEED": str(config.experiment.seed),
        "ROUNDS": str(config.experiment.rounds),
        "EXPERIMENT_TOTAL_NODES": str(config.total_nodes()),
        "NODE_ID": node_id,
        "NODE_TYPE": node_type_name,
        "NODE_INDEX": str(node_index),
        "NODE_SEED": str(derive_node_seed(config.experiment.seed, node_id)),
        "AGGREGATOR_ENDPOINT": str(config.aggregator.endpoint),  # AnyUrl -> str
        "INITIAL_PEERS": " ".join(initial_peers),
    }
    if identity_path is not None:
        env["IDENTITY_PATH"] = identity_path
    env.update(param_env(node_type.parameters))
    return env
