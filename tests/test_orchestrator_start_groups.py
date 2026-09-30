"""The start barrier is only as good as the cohorts the orchestrator publishes."""

from eclipse_hivemind.aggregator.models import ExperimentRegistration
from eclipse_hivemind.config import parse_config
from orchestrator.orchestrator import build_experiment_manifest, start_groups

BASE = """
schema_version: 1
experiment:
  name: groups
  seed: 1
  rounds: 2
aggregator:
  endpoint: http://aggregator:8080
bootstrap:
  seeds:
    node_type: normal
    count: 1
node_types:
  normal:
    count: 2
    image: node:dev
  adversarial:
    count: 1
    image: node:dev
"""

PHASES = """
startup:
  phases:
    - name: honest-network
      node_types: [normal]
      wait_after_seconds: 120
    - name: adversarial-nodes
      node_types: [adversarial]
"""


def test_without_phases_every_node_starts_in_one_group() -> None:
    config = parse_config(BASE)

    assert start_groups(config) == {"normal": "all", "adversarial": "all"}


def test_each_phase_is_its_own_group() -> None:
    config = parse_config(BASE + PHASES)

    assert start_groups(config) == {
        "normal": "honest-network",
        "adversarial": "adversarial-nodes",
    }


def test_a_node_type_in_no_phase_cannot_hold_another_group_closed() -> None:
    config = parse_config(
        BASE
        + """
startup:
  phases:
    - name: honest-network
      node_types: [normal]
"""
    )

    assert start_groups(config)["adversarial"] == "unscheduled:adversarial"


def test_manifest_labels_every_node_with_its_group() -> None:
    config = parse_config(BASE + PHASES)

    manifest = ExperimentRegistration.model_validate_json(
        build_experiment_manifest(config, "run-test")
    )

    assert {node.node_id: node.start_group for node in manifest.nodes} == {
        "normal-0": "honest-network",
        "normal-1": "honest-network",
        "adversarial-0": "adversarial-nodes",
    }
