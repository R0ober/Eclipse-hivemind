# Record whether hivemind actually averaged gradients

## The Problem

Training loss can decrease even when hivemind does not average with another
peer. When an averaging attempt fails, `hivemind.Optimizer` applies the local
gradients instead. An eclipse experiment therefore needs evidence of the
averaging outcome, not only loss and accuracy.

The optimizer does not expose that outcome through its public API.

## Options Considered

- Infer averaging from training metrics.
- Reimplement or subclass `hivemind.Optimizer`.
- Read private averager state around every step.
- **Read the optimizer's own status log lines.**

## Rationale

The optimizer logs `Averaged gradients with N peers` on success and `Proceeding
with local gradients` when it falls back. These lines describe what hivemind
actually did.

Metrics cannot distinguish successful averaging from local fallback. Replacing
or subclassing the optimizer would duplicate hivemind control flow and stop the
experiment from measuring the stock implementation. Private state is less stable
than the status messages we already depend on.

The log wording can change on a hivemind upgrade. Keeping the parsing in one
small, tested module makes that dependency explicit.

## Decision

- `node/shared/averaging_watch.py` captures status lines from
  `hivemind.optim.optimizer` during an epoch transition.
- Nodes emit `averaging_started` and `averaging_completed` events with
  `observed`, `success`, `fallback`, `fallback_reason`, `group_size`, and
  `duration_seconds`.
- The optimizer uses `verbose=True` so those messages are emitted at INFO level.
- Nodes pause for `step_delay_seconds` after each local step. The toy workload is
  otherwise fast enough for one peer to complete a round before it sees the
  others' progress.

## Notes

- `observed: false` means a node advanced by loading peer state, rather than by
  completing its own averaging attempt.
- `averaging_timeout` must be greater than `matchmaking_time`.
- Status: Accepted.
