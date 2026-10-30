# Designated-victim comparison

The project uses one notebook, [`analysis.ipynb`](../analysis.ipynb). This directory stores exported comparison tables, audited membership records and selected figures. Raw logs under ignored `outputs/` were used to verify membership and success; those logs are required to repeat the raw-event audit.

The comparison uses `normal-0` in both conditions, adversary counts 0–4 and 30 runs per cell. The six-adversary controls are excluded. The final comparison uses `final/` and the complete ordinary-matchmaking exports under `analysis/experiment-3/experiment-2/`. The ten-repeat checkpoint is retained locally and excluded from this results snapshot.

"With eclipse" is experimentally imposed gradient-group composition, not an implemented malicious eclipse mechanism. Only count four gives exclusively adversarial partners. Progress tracking and state sharing remain shared across groups. The treatment fixes both groups, so interpret it as an imposed-composition comparison rather than isolating a single protocol modification.

## Statistical summaries

Each observation is the final epoch-20 evaluation of `normal-0` from one run. Both conditions have 30 runs per adversary count. Repeat labels do not define paired randomised trials. Loss effects are differences and ratios of the two condition means; they are not means of pairwise run ratios.

The effect table uses 95% percentile bootstrap intervals, resampling whole runs independently within each condition with 5,000 samples and analysis seed 7. These descriptive intervals concern fixed-seed execution variability, not variability across data seeds; no multiple-comparison correction is applied. Boxplot whiskers use 1.5 IQR and are not confidence intervals. The trajectory figure shows medians and central 50% spread, not confidence bands.

## Outputs

- `paper/victim-loss-accuracy-comparison.pdf`: run-level victim loss and accuracy distributions.
- `paper/victim-loss-difference.pdf`: differences in mean victim loss, with run-resampling percentile bootstrap intervals.
- `paper/victim-loss-presentation.pdf`: presentation view of mean victim loss with and without eclipse, with bootstrap intervals of each condition mean.
- `paper/victim-loss-without-eclipse-presentation.pdf`: ordinary-matchmaking line alone on matching axes.
- `paper/victim-exposure-comparison.pdf`: observed successful-group exposure.
- `paper/victim-loss-trajectories.pdf`: median trajectories and central 50% spread.
- `membership-audit.csv`: wrong groups, missing membership and coverage for every run.
- `victim-performance-summary.csv` and `victim-loss-effects.csv`: numerical comparisons.
- `complete-coverage-sensitivity.csv`: descriptive results restricted to full successful victim coverage; the primary analysis retains all completed runs.

All captured nonempty enforced groups matched their prescribed identities. The four-adversary victim had 20 resolved successful records in all 30 runs. Other configurations include missing/unsuccessful observations, which remain reported. Manifest success denotes completed execution, not universal successful averaging.

Mean victim loss at four adversaries was 2.874 with imposed composition versus 1.286 without, a ratio of 2.234. These are means of the same designated victim across runs, not network means or post-selected worst nodes. Retain the variability and accuracy evidence when writing results. Collection sessions differ between conditions; fixed seeds and matching settings do not eliminate session effects.
