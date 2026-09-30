"""The averaging events are read out of hivemind's own log lines, so the parsing
of those lines is what has to be tested. The messages below are copied from
hivemind/optim/optimizer.py."""

import logging

from eclipse_hivemind.nodes.shared import averaging_watch


def records(*messages: str) -> list[tuple[float, str]]:
    return [(float(index), message) for index, message in enumerate(messages)]


def test_successful_round_reports_group_size() -> None:
    observation = averaging_watch.interpret(
        records(
            "Beginning optimizer step #3",
            "Averaged gradients with 4 peers",
            "Transitioning to epoch 4",
        )
    )

    assert observation["observed"] is True
    assert observation["success"] is True
    assert observation["fallback"] is False
    assert observation["fallback_reason"] is None
    assert observation["group_size"] == 4
    assert observation["duration_seconds"] == 1.0


def test_timeout_is_reported_as_a_fallback_with_its_reason() -> None:
    observation = averaging_watch.interpret(
        records(
            "Beginning optimizer step #3",
            "Averaging gradients failed with AllreduceException()",
            "Proceeding with local gradients",
        )
    )

    assert observation["success"] is False
    assert observation["fallback"] is True
    assert observation["fallback_reason"] == "AllreduceException()"
    assert observation["group_size"] is None


def test_single_peer_round_is_reported_as_a_fallback() -> None:
    observation = averaging_watch.interpret(
        records(
            "Beginning optimizer step #0",
            "Skipped averaging: there are no other peers",
            "Proceeding with local gradients",
        )
    )

    assert observation["fallback"] is True
    assert observation["fallback_reason"] == "no other peers"


def test_epoch_change_without_any_averaging_lines_is_marked_unobserved() -> None:
    observation = averaging_watch.interpret(records("Peer is out of sync"))

    assert observation["observed"] is False
    assert observation["success"] is False
    assert observation["duration_seconds"] is None


def test_watcher_captures_optimizer_lines_and_clears_them_when_taken() -> None:
    watcher = averaging_watch.attach()
    logger = logging.getLogger(averaging_watch.OPTIMIZER_LOGGER)
    try:
        logger.info("Averaged gradients with 2 peers")

        taken = watcher.take()
        assert [message for _created, message in taken] == ["Averaged gradients with 2 peers"]
        assert watcher.take() == []
    finally:
        logger.removeHandler(watcher)
