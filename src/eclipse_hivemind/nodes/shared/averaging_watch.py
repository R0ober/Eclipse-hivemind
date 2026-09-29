"""Read hivemind's own log lines to find out what an aggregation round actually did.

`hivemind.Optimizer` does not expose whether an epoch's gradients were averaged
with peers or whether it silently fell back to the local gradients. It only logs
it. The strings below are the exact messages from
`hivemind/optim/optimizer.py` (hivemind 1.1.12); the decisive one is
`Proceeding with local gradients`, logged by `_load_local_gradients_into_optimizer`
whenever averaging did not happen. See ADR 0013.

The messages are logged at `Optimizer.status_loglevel`, which is DEBUG unless the
optimizer is built with `verbose=True`, so a caller that wants them must pass
`verbose=True`.
"""

from __future__ import annotations

import logging
import re

OPTIMIZER_LOGGER = "hivemind.optim.optimizer"

ROUND_STARTED = "Beginning optimizer step #"
AVERAGED_GRADIENTS = re.compile(r"Averaged gradients with (\d+) peers")
AVERAGING_FAILED = re.compile(r"Averaging gradients failed with (.+)")
NO_OTHER_PEERS = "there are no other peers"
LOCAL_GRADIENTS = "Proceeding with local gradients"


class AveragingWatcher(logging.Handler):
    """Collect hivemind's optimizer status lines until someone takes them."""

    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: list[tuple[float, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append((record.created, record.getMessage()))

    def take(self) -> list[tuple[float, str]]:
        """Return the lines captured so far and start collecting again."""
        records, self.records = self.records, []
        return records


def attach(logger_name: str = OPTIMIZER_LOGGER) -> AveragingWatcher:
    """Start capturing the optimizer's status lines."""
    watcher = AveragingWatcher()
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)  # hivemind reads HIVEMIND_LOGLEVEL, which may be higher
    logger.addHandler(watcher)
    return watcher


def interpret(records: list[tuple[float, str]]) -> dict:
    """Summarise the log lines captured during one aggregation round."""
    started_at: float | None = None
    ended_at: float | None = None
    success = False
    group_size: int | None = None
    fallback = False
    reason: str | None = None

    for created, message in records:
        if started_at is None and ROUND_STARTED in message:
            started_at = created
        averaged = AVERAGED_GRADIENTS.search(message)
        if averaged is not None:
            success = True
            group_size = int(averaged.group(1))
            ended_at = created
        failed = AVERAGING_FAILED.search(message)
        if failed is not None:
            reason = failed.group(1)
        if NO_OTHER_PEERS in message:
            reason = "no other peers"
        if LOCAL_GRADIENTS in message:
            fallback = True
            ended_at = created

    if fallback and reason is None:
        reason = "averaging did not start"

    duration = None
    if started_at is not None and ended_at is not None:
        duration = ended_at - started_at

    return {
        "observed": started_at is not None,
        "started_at": started_at,
        "success": success,
        "group_size": group_size,
        "fallback": fallback,
        "fallback_reason": reason if fallback else None,
        "duration_seconds": duration,
    }
