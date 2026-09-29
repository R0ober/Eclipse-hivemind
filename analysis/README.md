# Analysis

```bash
pip install -e ".[analysis]"

python scripts/summarise_runs.py \
  --manifest outputs/experiment-1-manifest.csv --csv analysis/experiment-1/
```

`outputs/` is gitignored (raw logs). CSVs here are tracked. Same reason as
`measurements/dht-node.json`.

`runs.csv`: one row per cell, final state. Headline for exp 1 is
`honest_eval_loss` vs malicious count. Also has averaging success, group sizes,
fallbacks, who started, fingerprints, errors, sample counts. Read those before
loss. Failed averaging or mismatched start weights look like a successful attack.

`rounds.csv`: one row per run, round and node. Not aggregated. A mean can be a
model nobody has. Exp 2, 6 adversaries, honest losses 1.12, 4.84, 1.12, 1.12.
Mean 2.05. Three peers averaged together, one got isolated. Aggregate in the
notebook if you want.

Exp 1 is one group so the extra rows are duplicates. Full swarm cell is n=1
model. Error bars over those 10 nodes are zero.

Last round of `rounds.csv` rebuilds the final numbers in `runs.csv`. Averaging and
integrity columns are not per-round.

One run per cell. Seed fixes init, held-out set, data stream. Does not fix sample
counts. Round ends when the swarm-wide counter hits `target_batch_size`. Race
between containers. Exp 1 expected 25600 samples, got 29760 to 30336. Same config
again is a different model. Trend across 11 cells is usable. Exact cliff at 4 vs
5 adversaries is two draws. Repeat a cell if you care.

Use `eval_loss`. Accuracy floors at 0.25. Untrained and collapsed look the same.
`max_class_fraction` ~0.25 spread, aroubd 1.0 collapsed. Accuracy below 0.25 is
reversal once attackers outweigh honest peers. Honest and adversarial kept
separate. Small groups mean peers do not share weights. Swarm mean mixes in the
attacker.

`nbstripout --install` before committing notebooks. Finished figures should be a
script on the CSVs.
