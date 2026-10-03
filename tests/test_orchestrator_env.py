from eclipse_hivemind.config import parse_config
from orchestrator.env import build_node_env, derive_node_seed, param_env

CONFIG = """
schema_version: 1
experiment:
  name: env-test
  seed: 1
  rounds: 5
logger:
  endpoint: http://logger:8080
bootstrap:
  seeds:
    node_type: normal
node_types:
  normal:
    count: 2
    image: node:dev
  adversarial:
    count: 1
    image: node:dev
    parameters:
      learning_rate: 0.05
      target_group_size: 5
"""


def node_env(node_type_name: str = "normal", node_index: int = 0, **overrides) -> dict[str, str]:
    arguments = {
        "config": parse_config(CONFIG),
        "run_id": "run-test",
        "node_type_name": node_type_name,
        "node_index": node_index,
        "initial_peers": [],
        **overrides,
    }
    return build_node_env(**arguments)


def test_node_seed_derivation_is_pinned() -> None:
    """ADR 0005. If these numbers change, every node in every config gets a different
    seed and new runs stop being comparable with the ones already recorded."""
    assert derive_node_seed(1, "normal-0") == 6789994273556659625
    assert derive_node_seed(1, "normal-1") == 15382242029869153026
    assert derive_node_seed(2, "normal-0") == 15391305297234611652


def test_parameters_become_upper_case_param_variables() -> None:
    assert param_env({"target_prefix": "expert.", "batch_size": 32}) == {
        "PARAM_TARGET_PREFIX": "expert.",
        "PARAM_BATCH_SIZE": "32",
    }


def test_node_env_is_exactly_the_documented_contract() -> None:
    assert node_env("normal", 1) == {
        "EXPERIMENT_ID": "run-test",
        "EXPERIMENT_NAME": "env-test",
        "EXPERIMENT_SEED": "1",
        "ROUNDS": "5",
        "EXPERIMENT_TOTAL_NODES": "3",
        "NODE_ID": "normal-1",
        "NODE_TYPE": "normal",
        "NODE_INDEX": "1",
        "NODE_SEED": str(derive_node_seed(1, "normal-1")),
        "LOGGER_ENDPOINT": "http://logger:8080/",
        "INITIAL_PEERS": "",
    }


def test_node_type_parameters_are_added_for_that_node_type_only() -> None:
    adversarial = node_env("adversarial", 0)

    assert adversarial["PARAM_LEARNING_RATE"] == "0.05"
    assert adversarial["PARAM_TARGET_GROUP_SIZE"] == "5"
    assert not [key for key in node_env("normal", 0) if key.startswith("PARAM_")]


def test_initial_peers_are_space_separated() -> None:
    env = node_env(initial_peers=["/ip4/10.0.0.2/tcp/4000/p2p/a", "/ip4/10.0.0.3/tcp/4000/p2p/b"])

    assert env["INITIAL_PEERS"] == "/ip4/10.0.0.2/tcp/4000/p2p/a /ip4/10.0.0.3/tcp/4000/p2p/b"


def test_identity_path_is_only_set_when_given() -> None:
    assert "IDENTITY_PATH" not in node_env()
    assert node_env(identity_path="/identity/normal-0.key")["IDENTITY_PATH"] == "/identity/normal-0.key"


def test_every_value_is_a_string() -> None:
    """Docker takes the environment as strings; a stray int fails at container create."""
    assert all(isinstance(value, str) for value in node_env("adversarial", 0).values())
