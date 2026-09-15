# Terminate experiments by round count rather than wall-clock duration

## The Problem

Each experiment needs a clear stopping point that the system can detect reliably.

## Options Considered

- **Stop after a fixed number of rounds, where one averager step counts as one round**.
- Stop after a fixed amount of time.
- Stop when the nodes have reached a certain level of convergence.

## Rationale

Using a fixed number of rounds fits the way the system is already designed. One averager.step() is treated as one round.

Using rounds also makes experiments easier to reproduce. For example, running an experiment for 100 rounds gives every run the same amount of work, while running it for 10 minutes does not necessarily result in the same number of rounds.

The lifecycle component can also easily detect when a node has completed the required number of rounds. This gives us a clear signal that the experiment is finished.

Stopping based on convergence is more difficult because it may be hard to tell when the nodes have actually converged. It could also cause an experiment to run indefinitely, especially under adversarial conditions, which are exactly the conditions we want to test.

If coordinating the number of rounds across all nodes becomes difficult in practice, we can use a fixed time duration as a fallback.

## Notes

- `schema_version: 1` supports only the round-based termination mode for now.
- Other options, such as stopping after a certain duration or when the system converges, can be added later without changing the existing configuration format.
- Status: Accepted.
