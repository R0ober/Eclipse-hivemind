# With-eclipse controlled composition

This condition imposes gradient-averaging groups experimentally. It represents membership control, not a demonstrated malicious eclipse mechanism. Use "with eclipse" in plots with this qualification stated in Methodology.

Ten peers form two groups of five. The victim is `normal-0`. All adversaries and honest nodes with indices below `5 - adversaries` use the victim gradient namespace; the other honest nodes use a second gradient namespace. With zero through three adversaries the victim still has honest partners. Four adversaries give exclusively adversarial partners.

The node types remain `normal` and `adversarial`, preserving node IDs and per-node data seeds relative to the ordinary-matchmaking condition. Experiment-specific runners and images avoid changing the existing controls. Gradient prefixes differ; the optimizer run ID, global progress tracker, state averager namespace, target batch size and training settings remain shared as in the control. Cross-group state loading remains possible and must be reported, not silently treated as gradient isolation. The treatment fixes both groups, not just the victim's exposure.

## Build the experiment images

The ordinary honest and reverse-gradient images must already exist locally. These Dockerfiles reuse their installed dependencies and replace only the runner:

```bash
docker build --pull=false -f docker/node-honest-enforced.Dockerfile -t eclipse-hivemind-node-honest-enforced:dev .
docker build --pull=false -f docker/node-adversary-reverse-gradient-enforced.Dockerfile -t eclipse-adversary-reverse-gradient-enforced:dev .
```

The base tags are mutable. Record their image IDs when collecting a comparison; rebuilding a base image later does not establish identical dependencies to an archived run.

## Pilot gate

Run one repeat per configuration first. Validate exact captured member sets for every node and inspect success, fallback and final evaluations. A completion marker alone does not validate the treatment. Pilots are eligible as repeat 1 only if these checks pass. Do not silently discard failures or change settings between pilots and the full sweep.

```bash
python scripts/run_experiment.py configs/experiment-2-with-eclipse --repeat 1 --skip-existing
python scripts/verify_enforced_groups.py
```

After pilot checks and resource checks, collect 30 repeats per cell with the same directory and `--repeat 30 --skip-existing`. This reuses successful pilots. Run sequentially on the current host; two ten-peer runs need roughly 13 GB for peers alone, compared with roughly 9 GB initially available. Also confirm sufficient free disk space before the full sweep.

## Comparison

Use final `normal-0` evaluation loss and accuracy as the primary comparison against the corresponding ordinary-matchmaking runs. Keep adversary count and fixed seeds matched. Use membership and success to establish achieved exposure; do not assume settings guarantee successful averaging. Compare the zero-adversary treatment to the ordinary zero-adversary baseline to assess grouping effects unrelated to poisoning. Each condition fixes seed 7 across repeats, so repeated executions are not different data-seed trials.
