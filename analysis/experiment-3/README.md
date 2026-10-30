# Experiment 3: repeated runs

The project uses one notebook, [`analysis.ipynb`](../analysis.ipynb). This directory stores repeated-run exports, derived tables and figures separately from the initial Experiment 1 and 2 results. Raw logs remain under ignored `outputs/`.

## Collected runs

Experiment 1 contains 330 completed runs: 30 per adversarial count from zero to ten. Experiment 2 contains 180 completed runs: 30 per count at zero, one, two, three, four and six adversaries. Counts zero through four provide the ordinary-matchmaking controls; six remains supplementary. These are fixed-seed execution repeats, not different data-seed trials. All nodes have final evaluations at local optimizer epoch 20. Runs with averaging fallback remain included.

Regenerate the input tables from the local manifests:

```bash
python scripts/summarise_runs.py --manifest outputs/experiment-1-manifest.csv --csv analysis/experiment-3/experiment-1
python scripts/summarise_runs.py --manifest outputs/experiment-2-manifest.csv --csv analysis/experiment-3/experiment-2
```

`runs.csv` summarises each run; `rounds.csv` retains individual node evaluations; `membership.csv` resolves group PeerIDs. Missing identities are unknown rather than adversarial. Do not treat node observations from one run as independent repeats.

## Membership persistence

```bash
python scripts/analyse_group_persistence.py
```

The script checks raw averaging-completion success flags against resolved membership. It exports per-node, per-run and per-configuration tables, plus final `normal-0` measurements for a designated-victim comparison. Exact sorted member sets determine persistence; an unchanged adversarial count alone is insufficient. Missing, failed or unresolved observations break consecutive sequences. Complete persistence requires all 20 indices from zero to nineteen.

Raw event files are required to regenerate success-verified persistence tables. The checked-in tables remain readable without those files.

## Figures and interpretation

PDFs in `paper/` include final-loss distributions, exposure comparisons and group-persistence figures. The earlier smaller-group final-loss figure covers the original counts zero, two, four and six; the persistence tables cover all six collected configurations. Boxplots show quartiles and 1.5-IQR whiskers, not confidence intervals. Each run-level dot is one repeat; exposure scatterplots retain dependent node observations.

Mean and worst honest-node loss describe different outcomes. Mean loss can conceal concentrated harm. Inspect accuracy and prediction fractions before claiming collapse. Persistent adversarial-only groups under ordinary matchmaking do not establish an attacker-enforced eclipse mechanism. The all-adversarial Experiment 1 cell has no honest-node outcome.
