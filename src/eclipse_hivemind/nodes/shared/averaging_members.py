"""Remember which peers averaged together so the runner can log them.

This code has three jobs:
1. read_group_members reads the averaging result and extracts the peer IDs.
2. MemberCapturingOptimizer saves those IDs while Hivemind still has the result.
3. pop_group_members gives them to the runner, then clears them so an old group
   cannot be reported again.

Hivemind logs only the group size and clears the result before the runner can read
it. The subclass captures the IDs through a private method in Hivemind 1.1.12;
re-check that method when upgrading Hivemind.
"""

from __future__ import annotations

import hivemind


def read_group_members(step_control, timeout: float) -> list[str] | None:
    """Read the averaging result and extract the peer IDs in sorted order.

    step_control is the handle for this round's averaging operation. Its result
    is a dictionary keyed by peer ID. Return None if no member list is available:
    the operation is missing, has not been triggered, failed, or returned no peers.
    """
    if step_control is None or not getattr(step_control, "triggered", False):
        return None
    try:
        gathered = step_control.result(timeout)
    except BaseException:
        # Hivemind catches BaseException here too and falls back to local gradients.
        return None
    if not isinstance(gathered, dict) or not gathered:
        return None
    return sorted(str(peer_id) for peer_id in gathered)


class MemberCapturingOptimizer(hivemind.Optimizer):
    """Save the round's peer IDs before Hivemind discards the averaging result."""

    _last_members: list[str] | None = None

    def _average_gradients_and_load_into_optimizer(self, maybe_step_control):
        # Save the IDs for the runner, then let Hivemind handle the gradients.
        self._last_members = read_group_members(maybe_step_control, self.averaging_timeout)
        return super()._average_gradients_and_load_into_optimizer(maybe_step_control)

    def pop_group_members(self) -> list[str] | None:
        """Give the runner the saved peer IDs, then clear them to avoid logging them twice."""
        members = self._last_members
        self._last_members = None
        return members
