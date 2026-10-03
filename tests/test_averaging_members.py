"""The membership read is what makes an eclipse visible, so the logic that turns a
round's result into a peer list is tested on its own. The capture against a live
hivemind round needs a real multi-node swarm and is covered by an integration run,
not here."""

import sys
from pathlib import Path

import pytest

pytest.importorskip("hivemind")

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src" / "eclipse_hivemind" / "nodes"))

from shared import averaging_members  # noqa: E402


class FakeStepControl:
    """Stands in for hivemind's StepControl: a triggered round with a cached result."""

    def __init__(self, *, triggered: bool, result=None, raises: BaseException | None = None) -> None:
        self.triggered = triggered
        self._result = result
        self._raises = raises

    def result(self, _timeout):
        if self._raises is not None:
            raise self._raises
        return self._result


def test_members_are_the_sorted_peer_keys_of_the_round() -> None:
    gathered = {"12D3KooWBBB": object(), "12D3KooWAAA": object()}

    assert averaging_members.read_group_members(FakeStepControl(triggered=True, result=gathered), 5.0) == [
        "12D3KooWAAA",
        "12D3KooWBBB",
    ]


def test_no_control_means_no_membership() -> None:
    assert averaging_members.read_group_members(None, 5.0) is None


def test_an_untriggered_round_has_no_membership() -> None:
    assert averaging_members.read_group_members(FakeStepControl(triggered=False), 5.0) is None


def test_a_failed_round_has_no_membership() -> None:
    control = FakeStepControl(triggered=True, raises=RuntimeError("averaging failed"))

    assert averaging_members.read_group_members(control, 5.0) is None


def test_an_empty_round_has_no_membership() -> None:
    assert averaging_members.read_group_members(FakeStepControl(triggered=True, result={}), 5.0) is None


class PopOnly:
    """Exercises pop_group_members without constructing a real hivemind optimizer."""

    _last_members = None
    pop_group_members = averaging_members.MemberCapturingOptimizer.pop_group_members


def test_pop_group_members_returns_then_clears() -> None:
    optimizer = PopOnly()
    optimizer._last_members = ["12D3KooWAAA", "12D3KooWBBB"]

    assert optimizer.pop_group_members() == ["12D3KooWAAA", "12D3KooWBBB"]
    assert optimizer.pop_group_members() is None
