"""Plot only candidate visibility from the two completed SC cohorts.

Read final CSV exports and write a PNG to figures/. No experiment modules are imported.
"""
from pathlib import Path
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


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def require(condition, message):
    if not condition:
        raise ValueError(message)


def load(relative):
    directory = EXPERIMENT / "data" / relative
    paths = [directory / name for name in ("trials.csv", "discovery.csv")]
    hashes = {path: hashlib.sha256(path.read_bytes()).hexdigest() for path in paths}
    trials, snapshots = map(read_csv, paths)
    expected = {(f"c{c}-f{f}-p{p}", seed)
                for c, f, p in itertools.product((0, 1), repeat=3)
                for seed in range(17, 27)}
    require(len(trials) == 80 and
            {(r["cell"], int(r["seed"])) for r in trials} == expected,
            "Expected all 80 distinct cell/seed pairs")
    indexed = {r["run_id"]: r for r in trials}
    require(len(indexed) == 80, "Duplicate run ID")
    observations = {run_id: [] for run_id in indexed}
    for row in snapshots:
        require(row["run_id"] in indexed, "Unknown discovery run")
        trial = indexed[row["run_id"]]
        require((row["cell"], row["seed"]) == (trial["cell"], trial["seed"]),
                "Discovery identity mismatch")
        if row["recruiter_share"] == "":
            continue  # Empty/unknown snapshots are not zero recruiter shares.
        value = float(row["recruiter_share"])
        require(row["empty"] == "False" and row["unknown"] == "False"
                and math.isfinite(value) and 0 <= value <= 1,
                "Invalid evaluable candidate snapshot")
        observations[row["run_id"]].append(value)
    for trial in trials:
        require(trial["technical_valid"] == "True" and trial["endpoint_complete"] == "True",
                "Invalid/incomplete trial")
        values = observations[trial["run_id"]]
        require(len(values) == int(trial["candidate_known_nonempty"]) and values,
                "Missing evaluable candidate observations")
        require(math.isclose(statistics.fmean(values), float(trial["candidate_share"]),
                             abs_tol=1e-12), "Candidate mean differs from final trial export")
    return trials, hashes


def main():
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "dit616-candidate-mpl"))
    cache = Path(tempfile.gettempdir()) / "dit616-candidate-cache"
    cache.mkdir(exist_ok=True)
    os.environ.setdefault("XDG_CACHE_HOME", str(cache))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.patches import Patch
    import numpy as np

    plt.rcParams.update({"font.family": "serif", "font.serif": ["cmr10"],
                         "font.size": 10, "mathtext.fontset": "cm",
                         "axes.formatter.use_mathtext": True,
                         "pdf.fonttype": 42, "svg.fonttype": "none"})
    colors = ("#527c96", "#bf603d")
    fig, axes = plt.subplots(1, 2, figsize=(8.8, 4.1), sharey=True)
    fig.subplots_adjust(left=.08, right=.985, bottom=.25, top=.88, wspace=.10)
    source_hashes = {}
    for axis, (name, bucket, relative) in zip(axes, STUDIES):
        trials, hashes = load(relative)
        source_hashes.update(hashes)
        for index, (c, f) in enumerate(itertools.product((0, 1), repeat=2)):
            for p in (0, 1):
                subset = sorted((r for r in trials if r["cell"] == f"c{c}-f{f}-p{p}"),
                                key=lambda r: int(r["seed"]))
                values = np.array([100 * float(r["candidate_share"]) for r in subset])
                require(len(values) == 10, "Expected ten trials per condition")
                center = statistics.fmean(values)
                # Same bootstrap method, resample count and RNG seed as original plot.
                rng = np.random.default_rng(20261018)
                means = rng.choice(values, size=(5000, len(values)), replace=True).mean(axis=1)
                low, high = np.quantile(means, (.025, .975))
                x = index + (-.19 if p == 0 else .19)
                axis.bar(x, center, width=.34, color=colors[p], zorder=2)
                axis.errorbar(x, center, yerr=[[max(0, center-low)], [max(0, high-center)]],
                              color="#242424", capsize=3, linewidth=1, zorder=3)
                axis.scatter(x + np.linspace(-.08, .08, len(values)), values,
                             s=12, color="#242424", alpha=.65, zorder=4)
                print(f"{name} C={c} F={f} P={p}: {center:.4f}% (n={len(values)})")
        axis.set_title(f"{name} (bucket size {bucket})", fontsize=12, pad=10)
        axis.set_xticks(range(4), [f"C={c}, F={f}" for c, f in itertools.product((0, 1), repeat=2)])
        axis.set_ylim(0, 105)
        axis.set_yticks(range(0, 101, 20))
        axis.grid(axis="y", color="#d8dce0", linewidth=.55)
        axis.set_axisbelow(True)
        axis.spines[["top", "right"]].set_visible(False)
        for spine in ("left", "bottom"):
            axis.spines[spine].set_color("#b7bbc0")
    axes[0].set_ylabel("Malicious averagers among visible candidates (%)")
    fig.legend(handles=[Patch(facecolor=color, label=label)
                        for color, label in zip(colors, ("Standard gradients (P=0)", "Reversed gradients (P=1)"))],
               loc="lower center", bbox_to_anchor=(.5, .025), ncol=2, frameon=False)
    fig.savefig(DESTINATION / "candidate-visibility.png", dpi=240, facecolor="white")
    plt.close(fig)
    require(all(hashlib.sha256(path.read_bytes()).hexdigest() == digest
                for path, digest in source_hashes.items()), "Source data changed")
    print("Verified all 160 trial means against discovery snapshots; source hashes unchanged.")


if __name__ == "__main__":
    main()
