# Ordinary-matchmaking comparison

Complete the ten-peer, five-member-group controls at adversary counts 0, 1, 2, 3 and 4. Reuse the 30 recorded repeats for counts 0, 2 and 4, and collect 30 repeats each for counts 1 and 3. Keep the existing six-adversary cell as supplementary evidence rather than part of the new 0–4 comparison.

All cells use seed 7, 20 local optimizer epochs, batch size 32, target batch size 1280, learning rate 0.05, SGD momentum 0.9, evaluation size 4096, matchmaking time 10 seconds, averaging timeout 30 seconds and one-second local step delay. Target and minimum group sizes are both five. One honest bootstrap seed starts first; all nodes share one startup barrier. These are fixed-seed execution repeats, not samples across different data seeds.

The new configurations differ from the existing two-adversary configuration only in experiment name and honest/adversarial counts. All runs use ordinary Hivemind matchmaking and gradient negation. No victim admission or discovery manipulation is introduced.

Designate `normal-0` before examining the new outcomes as the future paired-comparison victim. Keep its loss separate from run-level mean and worst honest-node loss. The same victim designation and seed derivation should be used in a later manipulated-membership condition. Counts below four cannot give a node four adversarial partners in a group of five.

## Run and retain records

```bash
python scripts/run_experiment.py configs/experiment-2 --repeat 30 --skip-existing
```

The existing manifest identifies successful config/repeat pairs. This command adds the missing one- and three-adversary runs and skips the 120 existing runs. Failures remain in the manifest and are not replaced silently. The manifest status does not itself prove successful averaging.

Record image IDs and runner hashes alongside the sweep log. The checked images match the current honest and reverse-gradient runner files. Older runs used mutable image tags; archived configs alone cannot prove historical image identity.

## Performance and persistence analysis

Retain final honest-node loss, accuracy and prediction fractions. Report `normal-0`, the run-level honest mean and the worst honest-node value separately. Compare each configuration with the zero-adversary baseline. Do not classify ordinary-matchmaking outcomes as a deliberate eclipse.

For persistence, compare exact sorted member sets, not just adversarial counts. Join membership rows to raw averaging-completion success flags by run, node and round. Reject duplicate keys. Missing or failed observations break consecutive sequences.

- Complete persistence: a node has resolved, successful, five-member records for every index 0–19 with an identical member set. Summarise the fraction of runs where all honest nodes satisfy this criterion, as well as the per-node counts.
- Adjacent retention: among pairs of resolved successful observations at adjacent epoch indices, count identical member sets. Report the numerator and denominator. This is observation retention, not elapsed time.
- Longest sequence: count consecutive indices with an identical member set. Failed, missing or unresolved observations break the sequence.

Use runs as the unit for uncertainty estimates. Node observations within a run are dependent. Keep invalid observations and their coverage counts visible. Fixed seeds do not remove scheduling or matchmaking variability. Network size, node launch order and target batch size remain potential constraints on generalisation.
