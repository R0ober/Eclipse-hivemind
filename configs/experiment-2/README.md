# Experiment 2: averaging group size

Run the gradient-reversal attack with ten peers and groups of five. The four configs use zero, two, four or six malicious peers.

Each config sets `target_group_size` and `min_group_size` to 5. A round averages with five peers or falls back to local gradients. Check averaging success before comparing losses. See ADR 0016 in docs/decisions/.

An earlier check used six peers and groups of three. All 36 observed rounds averaged with three peers, with no fallbacks.

The settings fix group size, while matchmaking determines group membership. With four malicious peers among ten, a group of five contains between zero and four malicious peers. Use repeated runs to measure variation in composition and outcomes.

The `averaging_completed` event includes `group_members`. The `membership.csv` export resolves those PeerIDs through `node_started` events. Compare `honest_share_of_group` with each victim's evaluation loss. A group with unresolved PeerIDs has a blank share.

Run and summarise your sweep:

```bash
python scripts/run_experiment.py configs/experiment-2
python scripts/summarise_runs.py --manifest outputs/experiment-2-manifest.csv --csv analysis/experiment-2/
```
