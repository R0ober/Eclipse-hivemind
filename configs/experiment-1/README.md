# Experiment 1: malicious ratio sweep

Run 11 configs with ten peers each. The malicious peer count ranges from zero to ten. Adversarial peers reverse their gradients before averaging.

Each config sets `target_group_size` and `min_group_size` to 10. Every successful round includes the full swarm. Without these settings, matchmaking timing changes the group composition. See ADR 0016 in docs/decisions/.

This experiment measures gradient poisoning. Every peer participates in the same averaging group. Experiment 2 uses smaller groups.

Check `averaging_completed` before interpreting evaluation loss. A failed averaging round changes the training process. Use predicted_class_fractions to distinguish balanced predictions from single-class collapse when accuracy approaches 0.25.

The swarm-wide `target_batch_size` is 1,280. A value of 128 lets one peer supply enough samples to close an epoch. These configs run 20 rounds with seed 7.

Run and summarise your sweep:

```bash
python scripts/run_experiment.py configs/experiment-1
python scripts/summarise_runs.py --manifest outputs/experiment-1-manifest.csv --csv results/
```

Keep the manifest with your outputs. The orchestrator generates a new run ID for each launch. Use `--repeat` to collect multiple runs per config.
