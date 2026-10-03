# Analysis

Install the analysis dependencies and export experiment 1 results:

```bash
pip install -e ".[analysis]"
python scripts/summarise_runs.py --manifest outputs/experiment-1-manifest.csv --csv analysis/experiment-1/
```

Git excludes raw logs under outputs/. Track your result CSVs under analysis/.

## Read the tables

`runs.csv` contains one row per run. Compare honest_eval_loss across malicious peer counts. Check averaging success, group sizes, startup counts, model fingerprints, errors and sample counts before comparing loss.

`rounds.csv` contains one row per run, round and node. The script keeps each node's measurement separate. In one experiment 2 run with six adversaries, four honest peers reported losses of 1.12, 4.84, 1.12 and 1.12. Their mean was 2.05. Use per-node results to identify the isolated peer.

`membership.csv` records each averaging group's members as node IDs. Use `honest_share_of_group` to measure the group's composition. Unresolved PeerIDs keep their full strings and count as unknown. The script leaves the share blank when a group contains unknown members.

`topology.csv` records routing-table and nearest-target observations. Compare `tick_kind` and `occurred_at` before joining observations with training results. Membership round R precedes the training snapshot at local epoch R+1.

## Interpret your results

Experiment 1 averages the full swarm. Ten peer measurements from one run describe one shared model, rather than ten independent runs.

The seed fixes model initialisation, evaluation data and local data streams. Container scheduling affects the sample count before each epoch closes. Experiment 1 expected 25,600 samples in an earlier run and recorded totals from 29,760 to 30,336. Repeat your configs to measure run variation.

Use evaluation loss with predicted class fractions. Accuracy near 0.25 occurs with an untrained model or a model predicting one class. A maximum class fraction near 0.25 indicates balanced predictions. A value near 1.0 indicates concentration on one class. Keep honest and adversarial results separate.

## Notebook outputs

Register the output-stripping filter for your clone:

```bash
pip install -e ".[analysis]"
nbstripout --install
```

Your working notebook keeps its figures. Git stores the notebook without rendered outputs. Generate exported figures from the tracked CSVs.
