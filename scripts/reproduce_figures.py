"""Verify published data and reproduce the SC report and presentation figures."""
from pathlib import Path
import hashlib
import json
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]


def verify():
    provenance = json.loads((ROOT / "data/provenance.json").read_text())
    for study, record in provenance.items():
        for name, digest in record["data_sha256"].items():
            path = ROOT / "data" / study / name
            if hashlib.sha256(path.read_bytes()).hexdigest() != digest:
                raise ValueError(f"published data changed: {study}/{name}")


def main():
    verify()
    for name in ("plot_groupkey_ownership.py", "plot_candidate_visibility.py",
                 "plot_group_control.py", "plot_learning_outcomes.py"):
        subprocess.run([sys.executable, str(ROOT / "scripts/figures" / name)], cwd=ROOT, check=True)
    for name in ("plot_group_control.py", "plot_learning_outcomes.py"):
        subprocess.run([sys.executable, str(ROOT / "scripts/figures" / name), "--sc2-only"],
                       cwd=ROOT, check=True)
    verify()
    print("Seven report PNGs and two SC 2 presentation PNGs reproduced in figures/; "
          "published data checksums unchanged.")


if __name__ == "__main__":
    main()
