# Methods and data

## Design

Each study has ten seed blocks (17–26) and all eight C × F × P combinations
per block, in the recorded randomized order. A separate seed (10017) supplies
eight technical pilots. Each trial has one honest Victim, fourteen other
ordinary honest peers, three malicious averagers and two attacker support
peers. The logger and private coordinator are services, not trainers.

| Factor | Off | On |
| --- | --- | --- |
| C: coordination | Native matchmaking and admission | Attacker reservations/direct joins, selective admission, exact Victim+3 assembly and suppression of owner joins to a Victim leader |
| F: filtering | Standard DHT responses | All five controlled holders filter responses to the same three malicious averagers |
| P: poisoning | Standard gradients | The three malicious averagers negate their parameter gradients, g → −g |

Targeted placement of all five controlled identities is fixed in every cell,
including C0/F0/P0. Malicious identity and gradient behavior are distinct.
Coordination measures a policy bundle, not a single admission rule. Honest
peers do not consult the private coordination service.

The task is seeded synthetic four-class classification. All cells use the
same model, optimizer, batch sizes, learning rate, 4,096-point held-out
evaluation sample, target group size 4, minimum group size 2 and 12-epoch
horizon. SC 1 uses Hivemind's bucket size 20; SC 2 forwards bucket size 5 to
all twenty trainers. Both use five replicas and the native cache defaults.

## Tables and denominators

Each `data/sc*/` directory contains:

| File | Contents |
| --- | --- |
| `trials.csv` | One row per technically valid main trial, including endpoints, control metrics and coverage |
| `evaluations.csv` | Observed evaluation records for all training roles |
| `storage.csv` | Evaluated active announcement-record holder observations |
| `discovery.csv` | Victim candidate observations and coverage information |
| `groups.csv` | Observed Victim groups, outcome evidence and contribution-weight coverage |
| `contrasts.csv` | Conditional paired-seed C/F/P and coordination-by-poisoning contrasts |
| `technical-exclusions.csv` | Additional excluded attempt identities, reasons and checksums |

The six primary CSV exports per study preserve their original bytes. The
exclusion tables omit local configuration/archive paths, durations and raw
byte counts; they retain every excluded attempt, its original reason code,
cell/seed, run ID, status and configuration/archive checksums. The empty SC 1
exclusion table has a header for consistent machine reading.

Storage ownership is the attacker-owned holder fraction over active records
with complete observation coverage, averaged within a trial and then equally
across trials. Caches and incomplete observations are excluded; coverage is
reported separately. Candidate share uses unique malicious averager IDs in
known nonempty Victim snapshots. Empty and unknown snapshots are not zero
exposure and remain separate coverage cases.

Partner share excludes the Victim. A Victim+3-malicious group has 100%
malicious partners and 75% malicious membership overall. Successful exact
capture requires that exact membership and confirmed successful Victim
all-reduce, divided by all observed Victim groups. Unknown outcomes remain
unknown. Membership alone does not establish contribution weights or that
every member completed aggregation.

Learning outcomes are Victim argmax accuracy and cross-entropy loss at
optimizer epoch 12. Missing epochs are not interpolated. Figures use equally
weighted trial means, individual run dots and 95% percentile bootstrap
intervals from 5,000 resamples. They do not treat observation rows as independent
experimental repetitions.

## Technical exclusions in the published measurements

SC 1 contains 80 valid main trials and no additional main exclusions. SC 2
contains 80 valid main trials plus five excluded attempts:

| Cell | Seed | Reason |
| --- | ---: | --- |
| C1/F1/P0 | 18 | Native shutdown connection reset; cleanup occurred before every trainer completed |
| C0/F1/P1 | 19 | Completed-training DHT shutdown timeout |
| C1/F1/P0 | 20 | Completed-training DHT shutdown timeout |
| C1/F1/P1 | 21 | Completed-training DHT shutdown timeout |
| C1/F0/P0 | 22 | DHT resource-sharer teardown failure |

The first row is recorded under the historical reason code
`approved_native_shutdown_connection_reset`. It is a documented individual
post-start exclusion, not an all-trainer completed-training failure. The
shutdown-timeout handling was also amended after the campaign began.
Successful trials retained across operational continuations used identical
training images and scientific settings. Excluded attempts were preserved and
the same cell/seed repeated; failed records were not repaired into measurements.

The published results are conditional on technical completion. Equal training
settings do not eliminate possible bias from different failure rates across
conditions. A repeated seed has a new asynchronous schedule. The fresh-campaign
launcher retains the final bounded automatic classifiers, but does not import
historical successes or authorize the one-off incomplete-training exclusion.
An unknown failure stops and leaves its evidence for inspection.

## Instrumentation and provenance

SC 1 retains full diagnostic event payloads. SC 2 aggregates declared repeated
RPC diagnostics into exact per-second counts and retains primary learning,
Victim discovery, group, contribution, storage, error and completion records.
Source-side aggregation can change scheduling overhead. Consequently SC 1
versus SC 2 is a comparison of these two measured settings, not proof of a
bucket-size-only causal effect.

`provenance.json` links the included measurements to their original cohort,
image IDs, observed node dependencies and primary/excluded raw-archive SHA-256
identities. The current launchers create new locks for new runs. They cannot
resume the original machines' manifests. Historical image IDs document the
measurements; the publication Dockerfiles build independently.

`runtime-sha256.json` records publication runtime checksums. One personal comment
was generalized without changing executable code; `provenance.json` records both source hashes.
`config-sha256.json` records the original parsed configuration hashes after
normalizing image references; all other fields, including seeds and scientific
parameters, remain in the hash. The offline checks compare every supplied
configuration with both its generator and this measured-input reference.

These local fixed-budget toy-task studies do not establish a minimum attacker
count, whole-network corruption, continuous isolation between observations,
recovery, formal convergence speed or asymptotic divergence. Stronger observed
group capture does not by itself establish greater learning harm.
