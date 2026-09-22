# Start nodes in each phase together before training

## The Problem

Docker containers do not start at the same time. A seed node can finish its
training before the other nodes connect, which turns a swarm experiment into a
single-node run. Nodes in the same startup phase need to wait for each other
before their first training step.

ADR 0009 also allows later phases to join an already-running swarm. A barrier
for the whole experiment would remove that behaviour.

## Options Considered

- Sleep for a fixed time before every node starts training.
- Have the orchestrator release containers after it sees readiness logs.
- Wait for every node in the experiment to report `node_started`.
- **Wait for every node in the current startup phase to report `node_started`.**

## Rationale

A fixed sleep only guesses at startup time. The orchestrator could release
nodes, but the aggregator already has the manifest and receives `node_started`
events, so it can answer this question directly.

Waiting by phase keeps the meaning of `startup.phases`: nodes in one phase begin
together, while a later phase can still join an active swarm. The orchestrator
sets the groups in the manifest; the aggregator only checks whether all node IDs
in the asking node's group have started.

## Decision

- The experiment manifest records a `start_group` for each node. It is the
  startup phase name, or `all` when no phases are configured.
- The aggregator exposes
  `GET /api/v1/experiments/{id}/start-barrier?node_id=<node>`.
- A node sends `node_started`, then waits for every node in its own group before
  training.
- On timeout, a node continues and reports a recoverable `node_error` with
  `error_code: start_barrier_timeout`.

## Notes

- The barrier confirms that nodes started; it does not prove they can yet see
  each other in the DHT.
- A node type omitted from all phases is never started and cannot hold another
  group's barrier open.
- Status: Accepted.
