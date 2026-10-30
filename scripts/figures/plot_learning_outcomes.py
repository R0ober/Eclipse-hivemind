"""Plot Victim accuracy and loss at local optimizer epoch 12, SC1 versus SC2.

Read final CSV exports, check every endpoint against evaluations.csv, and write
two PNG figures to figures/. No experiment modules are imported.
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


def require(condition, message):
    if not condition:
        raise ValueError(message)


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def load(relative):
    directory = EXPERIMENT / "data" / relative
    paths = [directory / name for name in ("trials.csv", "evaluations.csv")]
    hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    trials, evaluations = map(read_csv, paths)
    expected = {(f"c{c}-f{f}-p{p}", seed)
                for c, f, p in itertools.product((0, 1), repeat=3)
                for seed in range(17, 27)}
    require(len(trials) == 80 and
            {(r["cell"], int(r["seed"])) for r in trials} == expected,
            "Expected all 80 distinct cell/seed pairs")
    indexed = {r["run_id"]: r for r in trials}
    require(len(indexed) == 80, "Duplicate trial ID")
    endpoints = {}
    for row in evaluations:
        require(row["run_id"] in indexed, "Unknown evaluation run")
        if row["node_id"] != "victim-0" or int(row["round"]) != 12:
            continue
        trial = indexed[row["run_id"]]
        require((row["cell"], row["seed"]) == (trial["cell"], trial["seed"]),
                "Evaluation identity mismatch")
        require(row["run_id"] not in endpoints, "Duplicate Victim endpoint")
        endpoints[row["run_id"]] = row
    require(endpoints.keys() == indexed.keys(), "Missing Victim endpoint")
    for trial in trials:
        require(trial["technical_valid"] == "True" and trial["endpoint_complete"] == "True",
                "Invalid/incomplete trial")
        for metric in ("accuracy", "loss"):
            value = float(endpoints[trial["run_id"]][metric])
            require(math.isfinite(value) and value >= 0, "Invalid evaluation value")
            if metric == "accuracy":
                require(value <= 1, "Invalid accuracy")
            require(math.isclose(value, float(trial[f"victim_{metric}"]), abs_tol=1e-12),
                    f"Victim {metric} differs from final evaluation")
    return trials, hashes


def plot(metric, studies):
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    import numpy as np

    scale = 100 if metric == "accuracy" else 1
    colors = ("#527c96", "#bf603d")
    fig, axes = plt.subplots(1, len(studies), figsize=(8.8, 4.1), sharey=True)
    axes = np.atleast_1d(axes)
    fig.subplots_adjust(left=.085, right=.985, bottom=.25, top=.82, wspace=.10)
    maximum = max(float(r[f"victim_{metric}"]) * scale for _, trials in studies for r in trials)
    for axis, ((name, bucket, _), trials) in zip(axes, studies):
        for index, (c, f) in enumerate(itertools.product((0, 1), repeat=2)):
            for p in (0, 1):
                subset = sorted((r for r in trials if r["cell"] == f"c{c}-f{f}-p{p}"),
                                key=lambda r: int(r["seed"]))
                require(len(subset) == 10, "Expected ten runs per condition")
                values = np.array([scale * float(r[f"victim_{metric}"]) for r in subset])
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
                print(f"Victim {metric}: {name} C={c} F={f} P={p}: {center:.6f}"
                      + ("%" if metric == "accuracy" else "") + " (n=10)")
        axis.set_title(f"{name} (bucket size {bucket})", fontsize=12, pad=10)
        axis.set_xticks(range(4), [f"C={c}, F={f}" for c, f in itertools.product((0, 1), repeat=2)])
        axis.set_ylim(0, 105 if metric == "accuracy" else maximum * 1.08)
        if metric == "accuracy":
            axis.set_yticks(range(0, 101, 20))
        axis.grid(axis="y", color="#d8dce0", linewidth=.55)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
        for spine in ("left", "bottom"):
            axis.spines[spine].set_color("#b7bbc0")
    axes[0].set_ylabel("Victim evaluation accuracy (%)" if metric == "accuracy"
                        else "Victim evaluation loss (cross-entropy)")
    fig.suptitle("Learning outcomes at local optimizer epoch 12", fontsize=12, y=.98)
    fig.legend(handles=[Patch(facecolor=color, label=label)
                        for color, label in zip(colors, ("Standard gradients (P=0)", "Reversed gradients (P=1)"))],
               loc="lower center", bbox_to_anchor=(.5, .025), ncol=2, frameon=False)
    suffix = "-sc2" if len(studies) == 1 else ""
    fig.savefig(DESTINATION / f"victim-{metric}{suffix}.png", dpi=240, facecolor="white")
    plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sc2-only", action="store_true",
                        help="Reproduce the single-panel SC 2 loss figure used for the presentation.")
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
    studies, hashes = [], {}
    for study in STUDIES[1:] if args.sc2_only else STUDIES:
        trials, source_hashes = load(study[2])
        studies.append((study, trials))
        hashes.update(source_hashes)
    for metric in ("loss",) if args.sc2_only else ("accuracy", "loss"):
        plot(metric, studies)
    require(all(hashlib.sha256(path.read_bytes()).hexdigest() == digest
                for path, digest in hashes.items()), "Source data changed")
    print(f"Verified {160 * len(studies)} trial endpoints against evaluations.csv; "
          f"{len(hashes)} source hashes unchanged.")


if __name__ == "__main__":
    main()
