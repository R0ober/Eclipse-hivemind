"""Create three SC1/SC2 membership, leadership and successful-capture figures.

Read final trial/group CSV exports, verify every plotted trial metric against
group records, and write PNGs to figures/. No experiment data is modified.
"""
from pathlib import Path
import argparse
import csv
import hashlib
import itertools
import math
import os
import statistics
import tempfile

EXPERIMENT = Path(__file__).resolve().parents[2]
DESTINATION = EXPERIMENT / "figures"
DESTINATION.mkdir(exist_ok=True)
STUDIES = (
    ("SC 1", 20, "sc1"),
    ("SC 2", 5, "sc2"),
)
METRICS = (
    ("partner_share", "formed-group-partners",
     "Malicious averagers among formed-group partners (%)"),
    ("recruiter_leader_rate", "recruiter-leadership",
     "Victim groups led by a malicious averager (%)"),
    ("successful_capture_fraction", "successful-exact-group-captures",
     "Confirmed successful exact group captures (%)"),
)


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def load(relative):
    directory = EXPERIMENT / "data" / relative
    paths = [directory / name for name in ("trials.csv", "groups.csv")]
    hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    trials, groups = map(read_csv, paths)
    expected = {(f"c{c}-f{f}-p{p}", seed)
                for c, f, p in itertools.product((0, 1), repeat=3)
                for seed in range(17, 27)}
    require(len(trials) == 80 and
            {(r["cell"], int(r["seed"])) for r in trials} == expected,
            "Expected all 80 distinct cell/seed pairs")
    indexed = {r["run_id"]: r for r in trials}
    require(len(indexed) == 80, "Duplicate trial ID")
    observations = {run_id: [] for run_id in indexed}
    identities = set()
    for row in groups:
        require(row["run_id"] in indexed, "Unknown group run")
        trial = indexed[row["run_id"]]
        require((row["cell"], row["seed"]) == (trial["cell"], trial["seed"]),
                "Group identity mismatch")
        identity = (row["run_id"], row["group_id"])
        require(identity not in identities, "Duplicate group record")
        identities.add(identity)
        share = float(row["recruiter_partner_share"])
        size = int(row["size"])
        require(size > 1 and math.isfinite(share) and 0 <= share <= 1,
                "Invalid group size/partner share")
        recruiter_count = share * (size - 1)
        require(math.isclose(recruiter_count, round(recruiter_count), abs_tol=1e-10)
                and recruiter_count <= 3 + 1e-10, "Invalid recruiter count")
        require(row["recruiter_leader"] in ("True", "False") and
                row["capture"] in ("True", "False"), "Undefined group classification")
        exact = size == 4 and math.isclose(share, 1, abs_tol=1e-12)
        require((row["capture"] == "True") == exact, "Exact-capture mismatch")
        observations[row["run_id"]].append(row)
    for trial in trials:
        require(trial["technical_valid"] == "True" and trial["endpoint_complete"] == "True",
                "Invalid/incomplete trial")
        rows = observations[trial["run_id"]]
        require(rows and len(rows) == int(trial["victim_groups"]),
                "Missing observed Victim groups")
        successful = sum(r["capture"] == "True" and r["allreduce"] == "success" for r in rows)
        require(successful == int(trial["successful_capture_groups"]),
                "Successful-capture count mismatch")
        calculated = {
            "partner_share": statistics.fmean(float(r["recruiter_partner_share"]) for r in rows),
            "recruiter_leader_rate": sum(r["recruiter_leader"] == "True" for r in rows) / len(rows),
            "successful_capture_fraction": successful / len(rows),
        }
        for metric, value in calculated.items():
            require(math.isclose(value, float(trial[metric]), abs_tol=1e-12),
                    f"{trial['run_id']}: {metric} differs from group records")
    return trials, hashes


def plot(metric, filename, ylabel, studies):
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    import numpy as np

    colors = ("#527c96", "#bf603d")
    fig, axes = plt.subplots(1, len(studies), figsize=(8.8, 4.1), sharey=True)
    axes = np.atleast_1d(axes)
    # A slightly wider left margin accommodates the longer partner-share label.
    fig.subplots_adjust(left=.085, right=.985, bottom=.25, top=.88, wspace=.10)
    for axis, ((name, bucket, _), trials) in zip(axes, studies):
        for index, (c, f) in enumerate(itertools.product((0, 1), repeat=2)):
            for p in (0, 1):
                subset = sorted((r for r in trials if r["cell"] == f"c{c}-f{f}-p{p}"),
                                key=lambda r: int(r["seed"]))
                require(len(subset) == 10, "Expected ten runs per condition")
                values = np.array([100 * float(r[metric]) for r in subset])
                center = statistics.fmean(values)
                rng = np.random.default_rng(20261018)
                means = rng.choice(values, size=(5000, len(values)), replace=True).mean(axis=1)
                low, high = np.quantile(means, (.025, .975))
                x = index + (-.19 if p == 0 else .19)
                axis.bar(x, center, width=.34, color=colors[p], zorder=2)
                axis.errorbar(x, center, yerr=[[max(0, center-low)], [max(0, high-center)]],
                              color="#242424", capsize=3, linewidth=1, zorder=3)
                axis.scatter(x + np.linspace(-.08, .08, len(values)), values,
                             s=12, color="#242424", alpha=.65, zorder=4)
                print(f"{metric}: {name} C={c} F={f} P={p}: {center:.4f}% (n=10)")
        axis.set_title(f"{name} (bucket size {bucket})", fontsize=12, pad=10)
        axis.set_xticks(range(4), [f"C={c}, F={f}" for c, f in itertools.product((0, 1), repeat=2)])
        axis.set_ylim(0, 105)
        axis.set_yticks(range(0, 101, 20))
        axis.grid(axis="y", color="#d8dce0", linewidth=.55)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
        for spine in ("left", "bottom"):
            axis.spines[spine].set_color("#b7bbc0")
    axes[0].set_ylabel(ylabel)
    fig.legend(handles=[Patch(facecolor=color, label=label)
                        for color, label in zip(colors, ("Standard gradients (P=0)", "Reversed gradients (P=1)"))],
               loc="lower center", bbox_to_anchor=(.5, .025), ncol=2, frameon=False)
    fig.savefig(DESTINATION / f"{filename}.png", dpi=240, facecolor="white")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sc2-only", action="store_true",
                        help="Reproduce the single-panel SC 2 capture figure used for the presentation.")
    args = parser.parse_args()
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "dit616-candidate-mpl"))
    cache = Path(tempfile.gettempdir()) / "dit616-candidate-cache"
    cache.mkdir(exist_ok=True)
    os.environ.setdefault("XDG_CACHE_HOME", str(cache))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    plt.rcParams.update({"font.family": "serif", "font.serif": ["cmr10"],
                         "font.size": 10, "mathtext.fontset": "cm",
                         "axes.formatter.use_mathtext": True,
                         "pdf.fonttype": 42, "svg.fonttype": "none"})
    studies = []
    hashes = {}
    for study in STUDIES[1:] if args.sc2_only else STUDIES:
        trials, source_hashes = load(study[2])
        studies.append((study, trials))
        hashes.update(source_hashes)
    for metric, filename, ylabel in METRICS[-1:] if args.sc2_only else METRICS:
        if args.sc2_only:
            filename += "-sc2"
        plot(metric, filename, ylabel, studies)
    require(all(hashlib.sha256(path.read_bytes()).hexdigest() == digest
                for path, digest in hashes.items()), "Source data changed")
    print(f"Verified {240 * len(studies)} trial metric values against group records; "
          f"{len(hashes)} source hashes unchanged.")


if __name__ == "__main__":
    main()
