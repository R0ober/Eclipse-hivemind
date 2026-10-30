# Experiment 3: repeated runs

Repeat the poisoning configurations to measure variation between executions. The fixed experiment seed controls model initialization and evaluation data; node-specific seeds control training batches. Scheduling, sample accumulation and matchmaking still vary between runs.

## Recorded collection

The checked-in tables contain 30 completed runs per configuration:

- Experiment 1: 11 adversary counts, zero through ten, for 330 runs.
- Experiment 2: counts zero, one, two, three, four and six, for 180 runs. Counts zero through four are the ordinary-matchmaking comparison; six is supplementary.

The earlier proposal targeted 100 repeats per Experiment 2 configuration. The recorded collection uses 30; do not report the earlier target as completed work. Each run, rather than each peer within a run, is a repeated experimental observation.

This directory contains study documentation rather than additional YAML configurations. Reuse the experiment directories:

```bash
python scripts/run_experiment.py configs/experiment-1 --repeat 30 --skip-existing
python scripts/run_experiment.py configs/experiment-2 --repeat 30 --skip-existing
```

The local manifests identify completed config/repeat pairs. Without the original manifests, these commands collect new runs rather than reconstructing the archived executions.

## Export and analyse

```bash
python scripts/summarise_runs.py --manifest outputs/experiment-1-manifest.csv --csv analysis/experiment-3/experiment-1
python scripts/summarise_runs.py --manifest outputs/experiment-2-manifest.csv --csv analysis/experiment-3/experiment-2
python scripts/analyse_group_persistence.py
```

Read [the repeated-run analysis guide](../../analysis/experiment-3/README.md) for table definitions and interpretation. The project uses [analysis.ipynb](../../analysis/analysis.ipynb) as its notebook. Raw event files are required to regenerate success-verified persistence measurements; the checked-in CSVs remain available without them.

Report sample counts and spread, retain per-node outcomes and distinguish loss from prediction collapse. Ordinary-matchmaking membership persistence is not evidence of an implemented attacker-controlled isolation mechanism. The imposed-group comparison is documented [separately](../experiment-2-with-eclipse/README.md).

## Resources

Runs were collected sequentially on a single host. A ten-peer run used approximately 6.4 GiB for the experiment containers. Check available memory and disk before another sweep and preserve failed-run records. Fixed seeds do not remove host-session effects.

The earlier 1,000-peer scaling proposal is separate from this repeated-run study. See [ADR 0007](../../docs/decisions/0007-node-count-ceiling-single-host.md).
