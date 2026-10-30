"""Check the prescribed treatment without launching distributed training."""

import importlib.util
import sys
from pathlib import Path

import pytest


pytest.importorskip("torch")
pytest.importorskip("hivemind")
NODES = Path(__file__).resolve().parents[1] / "src" / "eclipse_hivemind" / "nodes"
sys.path.insert(0, str(NODES))


@pytest.fixture(params=["honest-enforced", "adversary-reverse-gradient-enforced"])
def runner(request):
    """Load each experiment-owned runner as its container would."""
    spec = importlib.util.spec_from_file_location("enforced_runner", NODES / request.param / "runner.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("adversaries", range(5))
def test_victim_receives_all_adversaries_in_two_groups_of_five(runner, adversaries):
    """Verify the experimental assignment for every tested population."""
    groups = {"victim": set(), "other": set()}
    for role, count in [("normal", 10 - adversaries), ("adversarial", adversaries)]:
        for index in range(count):
            group = runner.enforced_group({"node_type": role, "node_index": index}, adversaries)
            groups[group].add((role, index))
    assert len(groups["victim"]) == len(groups["other"]) == 5
    assert groups["victim"].isdisjoint(groups["other"])
    assert ("normal", 0) in groups["victim"]
    assert sum(role == "adversarial" for role, _ in groups["victim"]) == adversaries
    assert all(role == "normal" for role, _ in groups["other"])


@pytest.mark.parametrize("adversaries", [-1, 5])
def test_out_of_scope_populations_are_rejected(runner, adversaries):
    with pytest.raises(ValueError, match="between zero and four"):
        runner.enforced_group({"node_type": "normal", "node_index": 0}, adversaries)


def test_gradient_namespaces_are_separate_and_keep_averager_options(runner, monkeypatch):
    """Check the factory receives distinct prefixes while retaining Hivemind options."""
    monkeypatch.setattr(runner, "GradientAverager", lambda **kwargs: kwargs)
    victim = runner.create_enforced_gradient_averager(
        prefix="run_grad_averager", group="victim", target_group_size=5, min_group_size=5)
    other = runner.create_enforced_gradient_averager(
        prefix="run_grad_averager", group="other", target_group_size=5, min_group_size=5)
    assert victim["prefix"] != other["prefix"]
    assert victim["prefix"].startswith("run_grad_averager")
    assert victim["target_group_size"] == other["target_group_size"] == 5
    assert victim["min_group_size"] == other["min_group_size"] == 5
