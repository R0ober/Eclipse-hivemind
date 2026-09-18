# Let nodes send events and let the aggregator produce experiment results

## The Problem

The nodes need to report what happened during an experiment. That includes
startup and shutdown, resource use, training metrics, averaging activity, and
errors. We also want to compare nodes, node types, and attack behaviour after a
run has finished.

If nodes produce their own summaries, each node has to know which results matter
and how to format them. That makes the experiment code harder to change and can
make nodes report different things. It also makes it harder to add a new metric
without changing every node.

We need a small API between nodes and the aggregator that keeps the raw facts
from the experiment and leaves the human-readable result to one place.

## Options Considered

- Have nodes write files and collect the files after the experiment.
- Have nodes send a fixed metrics object to the aggregator.
- **Have nodes send versioned, typed events to the aggregator, which stores them
  and produces the experiment result.**
- Send all node logs to the aggregator and parse them afterwards.

## Rationale

Events match what the experiment observes. A node can report that it started,
completed an averaging step, measured its memory, or failed without the API
having to become one large fixed metrics object.

The event envelope stays the same while the data inside it depends on the event
type. This gives us a stable API for the node and room to add metrics later.

The aggregator is the right place to combine events. It can compare honest and
adversarial nodes, calculate timings, detect missing nodes, and export a result
without making every node implement analysis code.

The API uses at least once delivery. Nodes may retry a batch, and the
aggregator removes duplicates using the experiment ID and event ID. This is
simpler and more reliable than trying to guarantee exactly once delivery over a
network.

## Decision

Nodes send batches of events to:

```text
POST /api/v1/experiments/{experiment_id}/events
```

Every event has an event ID, sequence number, type, timestamp, and type-specific
data. Every request identifies the node that produced the events. The
aggregator validates the experiment and node before storing the events.

The first event types are:

- `node_started`
- `resource_sample`
- `training_metrics`
- `averaging_started`
- `averaging_completed`
- `node_finished`
- `node_error`

The full request and event format is documented in
[the aggregator API reference](../aggregator-api-reference.md).

## Notes

- The aggregator stores raw events as the source of truth. Human-readable
  summaries and machine-readable exports are derived from those events.
- A node sends `experiment_id`, `node_id`, `node_type`, and `node_index` with
  each batch. The aggregator checks these against the experiment manifest
  instead of trusting a node to identify itself correctly.
- Version 1 does not add authentication. The aggregator is kept on the private
  Docker network for an experiment, and the manifest check prevents unknown
  experiments and nodes from being accepted. Authentication can be added if
  the aggregator is later shared between hosts or exposed outside that network.
- Nodes should batch events instead of making one request for every training
  step. A first implementation should flush by time or batch size, for example
  every 1 to 5 seconds or every 50 events.
- The first storage implementation can use SQLite in WAL mode. The event table
  needs indexes by experiment, node, event type, and timestamp. PostgreSQL is a
  later option if concurrent experiments or write volume make SQLite unsuitable.
- JSON Lines is the first export format because it keeps event-specific data
  without forcing every event type into a wide table. CSV or Parquet can be
  added when the analysis workflow needs them.
- The API version is part of the URL. Breaking changes use a new version rather
  than silently changing the meaning of an existing event.
- Status: Proposed.
