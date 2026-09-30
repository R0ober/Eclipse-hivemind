"""The task is what makes an attacked run readable, so its properties are tested
rather than assumed: a balanced class prior, and an evaluation that distinguishes
a model which has learned nothing from one that has collapsed onto a single class.

Each node type owns its own task (ADR 0011), so this tests the honest node's. A
node type that deliberately trains something else gets its own test rather than
being held to this one.

The runner is imported the way the container sees it: `shared/` sits next to the
runner on the image, so `nodes/` goes on the path rather than the package root.
"""

import sys
from pathlib import Path

import pytest

# The runner needs the full node stack. Without it this file skips rather than
# fails, so the rest of the suite can run on a machine that has neither.
torch = pytest.importorskip("torch")
pytest.importorskip("hivemind")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "eclipse_hivemind" / "nodes"))

from honest import runner  # noqa: E402


def test_classes_are_balanced() -> None:
    """A collapsed model scores the class prior, so the prior has to be 1/4 for
    0.25 to read as 'guessing one class' on a graph."""
    _features, labels = runner.generate_batch(8192, torch.Generator().manual_seed(7))

    for class_index in range(runner.NUM_CLASSES):
        fraction = (labels == class_index).float().mean().item()
        assert abs(fraction - 0.25) < 0.02


def test_batch_is_reproducible_from_the_generator() -> None:
    """Every node builds the held-out set from the experiment seed alone (ADR 0014),
    so the same seed has to give the same data in every container."""
    features, labels = runner.generate_batch(64, torch.Generator().manual_seed(11))
    again_features, again_labels = runner.generate_batch(64, torch.Generator().manual_seed(11))

    assert torch.equal(features, again_features)
    assert torch.equal(labels, again_labels)


def test_evaluate_reports_the_prediction_spread() -> None:
    features, labels = runner.generate_batch(256, torch.Generator().manual_seed(3))
    _loss, _accuracy, fractions = runner.evaluate(runner.build_model(), features, labels)

    assert len(fractions) == runner.NUM_CLASSES
    assert abs(sum(fractions) - 1.0) < 1e-5


def test_a_collapsed_model_is_visible_in_the_prediction_spread() -> None:
    """The signature of a successful gradient-reversal attack: accuracy sits at the
    0.25 class prior while every prediction falls into one class."""

    class AlwaysClassTwo(torch.nn.Module):
        def forward(self, features: torch.Tensor) -> torch.Tensor:
            logits = torch.zeros(features.shape[0], runner.NUM_CLASSES)
            logits[:, 2] = 10.0
            return logits

    features, labels = runner.generate_batch(4096, torch.Generator().manual_seed(5))
    _loss, accuracy, fractions = runner.evaluate(AlwaysClassTwo(), features, labels)

    assert fractions[2] == 1.0
    assert abs(accuracy - 0.25) < 0.02
