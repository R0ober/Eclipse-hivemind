# Experiment 1 — malicious ratio sweep

How much gradient reversal a 10-peer swarm can take. Eleven cells, 0 through 10
adversaries.

Averaging is locked to the whole swarm (`target_group_size` and `min_group_size`
both 10, ADR 0016). Otherwise hivemind groups whoever arrived in time and the
ratio in the filename stops meaning anything. Everyone starts together,join
order is a different experiment.

This is a poisoning experiment, not eclipse. Everyone still averages with everyone. Experiment
2 is the same attack with smaller groups.

Trust `eval_loss`. Accuracy floors at 0.25, so collapse and "never learned" look
the same; `predicted_class_fractions` tells them apart. Check
`averaging_completed` first — a broken swarm and a successful attack have the
same loss curve.

`target_batch_size: 1280` is swarm-wide (10 peers × 128 samples). Set it to 128
and the round ends after one peer. 20 rounds is enough for collapse; 10 isn't.

```bash
python scripts/run_experiment.py configs/experiment-1
python scripts/summarise_runs.py --manifest outputs/experiment-1-manifest.csv --csv results/
```

Seed 7, one run per cell. Run ids are generated at launch (ADR 0001) — keep the
manifest with the outputs.
