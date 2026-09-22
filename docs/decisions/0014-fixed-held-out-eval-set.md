# Evaluate every node on the same held-out data

## The Problem

The per-step accuracy is measured on one node's current training batch. It is
noisy, differs between nodes, and cannot show whether an attack made an honest
node's model worse.

We need an evaluation result that is comparable between nodes and between runs.

## Options Considered

- Smooth the local batch accuracy during analysis.
- Give every node a separate held out set.
- **Generate one held out set from the experiment seed and use it on every
  node.**

## Rationale

A shared set gives every node the same yardstick. Nodes with the same weights
then report the same loss and accuracy; a difference is evidence that their
models diverged.

Smoothing batch accuracy hides noise without making the samples comparable.
Separate evaluation sets have the same problem. Evaluating after each global
epoch matches the point where the swarm model may have changed.

## Decision

- Generate `eval_size` samples from `experiment.seed` for every node.
- Initialise the model from `experiment.seed` and record its fingerprint in
  `node_started`.
- Emit `eval_metrics` before training and after each epoch transition.
- Keep the local per-step value as `batch_accuracy` so it cannot be confused
  with the shared evaluation result.

## Notes

- Training batches still use the node-specific seed.
- The generated evaluation set is not a strict train/test split, although an
  exact duplicate from a continuous random stream is unlikely.
- Status: Accepted.
