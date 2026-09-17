# Keep startup timing in its own config block, separate from node types

## The Problem

When an attacker joins changes what the experiment measures. If adversarial nodes
start at the same time as the honest ones, the honest network never forms first, and
the run tests a different situation from the one we care about. Join order has to be
something the config controls.

## Options Considered

- Start every node at the same time.
- Add a delay or `start_after` field to each node type.
- **Add a separate `startup` block listing phases, each naming node types and a wait**.

## Rationale

Join order is experiment timing, not node behaviour. Keeping it in its own block
means the same node type definitions can be reused with different schedules, and a
schedule can change without touching what a node does. This is the same separation
ADR 0004 makes for behaviour, applied to time.

Putting a delay on each node type mixes the two concerns and makes the order
implicit. You would have to compare delay values across every type to work out what
happens first. A phase list states the order directly and reads as a plan.

Starting everything at once is still available by leaving `startup` out, which is
the default.

## Notes

- Seed nodes start before the first phase, because every other node needs their
  addresses. The phase list controls the order of everything after that.
- When phases are present, a node type not named in any phase is never started. The
  parser does not currently warn about this, so a typo silently drops a node type.
  Worth adding a validation rule.
- `wait_after_seconds` is a fixed delay, not a condition. A more precise version
  would wait until the honest network has actually converged. Deferred until we can
  measure convergence, see ADR 0006.
- Status: Accepted.
