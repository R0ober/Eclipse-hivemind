# Storage Control experiments in Hivemind

Code, measured data and report figures for **SC 1 (factorial-1)** and **SC 2 (factorial-2)**.

## Configuration

| Study | DHT bucket size | Diagnostic logging | Main trials |
| --- | ---: | --- | ---: |
| SC 1 | 20 | Full | 80 |
| SC 2 | 5 | Compact | 80 |

Shared settings:

- Hivemind 1.1.12, PyTorch 2.14.1; synthetic four-class classification on CPU.
- 20 trainers: 15 honest peers including the Victim, three malicious averagers and two attacker support peers.
- Five targeted attacker identities, five DHT replicas; target/minimum averaging group size 4/2; 12 optimizer epochs.
- SGD: learning rate 0.05, momentum 0.9; local/target batch size 32/1280; 4,096 evaluation samples.
- Coordination × response filtering × gradient sign flipping: eight conditions, seeds 17–26. Eight separate technical pilots use seed 10017.

## Contents

- `configs/sc1/`, `configs/sc2/`: pilot and main configurations.
- `src/`, `docker/`: training, attack, logging, orchestration and container builds.
- `scripts/`: image setup, experiment launchers and plotting.
- `data/sc1/`, `data/sc2/`: measured CSVs; `data/*.json`: checksums and provenance.
- `checks/`: validation tests. [Methods](docs/methods.md) documents metrics, exclusions and instrumentation differences.

## Reproduce

From the repository root, using Python 3.12+:

```sh
python3 -m venv .venv
.venv/bin/python -m pip install -r requirements.txt
.venv/bin/python scripts/reproduce_figures.py
```

This recreates seven report figures and the two SC 2 presentation figures as PNGs
in `figures/` from the included CSVs.

For a new campaign, start Docker on Linux or macOS, then run:

```sh
.venv/bin/python scripts/prepare_images.py sc1
.venv/bin/python scripts/sc1/run.py --phase pilot --resume
.venv/bin/python scripts/sc1/run.py --phase main --resume
.venv/bin/python scripts/sc1/report.py
```

Replace `sc1` with `sc2` for SC 2. New results go to `outputs/sc1/` and raw archives
to `archives/sc1/raw/` (likewise for SC 2). Use a fresh checkout for another campaign.

License: [MIT](LICENSE).
