# Experiment 3: repeated runs

## Goal

Measure execution variation in the gradient-poisoning experiments and relate individual training outcomes to observed averaging membership.

## Recorded status

The repeat and resume tooling is implemented. The recorded collection contains 330 Experiment 1 runs and 180 Experiment 2 runs, with 30 completed runs per configuration. The original Experiment 2 cells were extended with one and three adversaries. The earlier target of 100 repeats per Experiment 2 cell was not used for this snapshot.

Input tables, manifest snapshots and selected figures are in [analysis/experiment-3](../../analysis/experiment-3/README.md). The persistence script verifies success against raw event records and reports coverage, exact partner retention and unchanged sequences. The project keeps one analysis notebook.

## Reproduce collection and exports

Use [the study guide](../../configs/experiment-3/README.md) for repeat commands and export locations. Resume uses local manifest records; archived tables do not substitute for those manifests. Preserve interrupted attempts and keep averaging failures in the performance analysis unless an exclusion is explicitly justified.

## Interpretation

Use runs as the unit for comparisons. Averages across honest peers can conceal a high-loss individual. Membership and successful averaging must be checked separately from local optimizer epoch advancement. Report persistence under ordinary matchmaking separately from deliberately imposed membership.

## Further work

Any larger repeat collection should be labelled as an extension. Different data seeds and additional hosts would test variability beyond the fixed-seed, single-host conditions collected here. Keep those changes distinct from the existing dataset.
