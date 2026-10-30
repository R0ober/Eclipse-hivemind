"""Plot measured group-key replica ownership in both completed factorial cohorts.

Run with the repository's analysis environment; no experiment modules are imported:
    .venv/bin/python scripts/figures/plot_groupkey_ownership.py

Ownership is the mean attacker-holder fraction over active announcement-record
observations with complete snapshot coverage, first within each trial, then
equally across trials. Cache records and incomplete observations are excluded by
the original exporter. Coverage uses snapshot counts in trials.csv, because
storage.csv has one row per record and omits snapshots with no records.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import itertools
import json
import math
import os
from pathlib import Path
import statistics
import tempfile


ROOT = Path(__file__).resolve().parents[2]
CELLS = tuple(f"c{c}-f{f}-p{p}" for c, f, p in itertools.product((0, 1), repeat=3))
STUDIES = (
    ("SC 1", 20, "sc1"),
    ("SC 2", 5, "sc2"),
)
DEFAULT_OUTPUT = ROOT / "figures"


def require(condition, message):
    if not condition:
        raise ValueError(message)


def sha256(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_csv(path):
    with path.open(newline="") as stream:
        return list(csv.DictReader(stream))


def write_csv(path, rows):
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def read_study(base, name, bucket_size, relative):
    directory = base / relative
    paths = [directory / file for file in ("trials.csv", "storage.csv")]
    initial_hashes = {str(path.resolve()): sha256(path) for path in paths}
    trials, records = (read_csv(path) for path in paths)
    expected = set(itertools.product(CELLS, range(17, 27)))
    require(len(trials) == 80, f"{name}: expected 80 main trials")
    require({(r["cell"], int(r["seed"])) for r in trials} == expected,
            f"{name}: incomplete or duplicate factorial cells/seeds")
    indexed = {r["run_id"]: r for r in trials}
    require(len(indexed) == 80, f"{name}: duplicate trial IDs")
    values = {run: [] for run in indexed}
    for record in records:
        trial = indexed.get(record["run_id"])
        require(trial is not None, f"{name}: storage record has unknown trial")
        require((record["cell"], record["seed"]) == (trial["cell"], trial["seed"]),
                f"{name}: storage record identity mismatch")
        if record["attacker_holder_share"] == "":
            continue  # Inactive records and/or incomplete snapshots; never impute.
        share = float(record["attacker_holder_share"])
        holders = int(record["holders"])
        require(record["complete_coverage"] == "True" and holders > 0,
                f"{name}: defined ownership without complete coverage/holders")
        require(math.isfinite(share) and 0 <= share <= 1,
                f"{name}: invalid ownership fraction")
        require(math.isclose(share * holders, round(share * holders), abs_tol=1e-9),
                f"{name}: ownership fraction inconsistent with holder count")
        values[record["run_id"]].append(share)

    result = []
    for trial in sorted(trials, key=lambda r: (r["cell"], int(r["seed"]))):
        require(trial["technical_valid"] == "True" and trial["endpoint_complete"] == "True",
                f"{name}: invalid or incomplete trial")
        observations = values[trial["run_id"]]
        require(bool(observations), f"{name}: trial has no evaluable ownership observations")
        ownership = statistics.fmean(observations)
        require(math.isclose(ownership, float(trial["attacker_holder_share"]), abs_tol=1e-12),
                f"{name}: recomputed ownership differs from trials.csv")
        samples = int(trial["storage_samples"])
        complete = int(trial["complete_storage_samples"])
        require(0 < samples and 0 <= complete <= samples, f"{name}: invalid coverage counts")
        coverage = complete / samples
        require(math.isclose(coverage, float(trial["complete_storage_fraction"]), abs_tol=1e-12),
                f"{name}: inconsistent coverage summary")
        result.append(dict(
            study=name, bucket_size=bucket_size, cell=trial["cell"], seed=int(trial["seed"]),
            run_id=trial["run_id"], ownership_percent=100 * ownership,
            coverage_percent=100 * coverage, storage_samples=samples,
            complete_storage_samples=complete, evaluable_record_observations=len(observations),
            fully_owned_record_observations=sum(v == 1 for v in observations),
            partly_or_not_owned_record_observations=sum(v < 1 for v in observations),
            minimum_record_ownership_percent=100 * min(observations),
        ))
    require(initial_hashes == {str(path.resolve()): sha256(path) for path in paths},
            f"{name}: inputs changed during reading")
    return result, initial_hashes


def summarize(rows, *, by_cell):
    result = []
    for name, bucket, _ in STUDIES:
        for cell in CELLS if by_cell else (None,):
            subset = [r for r in rows if r["study"] == name and (cell is None or r["cell"] == cell)]
            result.append(dict(
                study=name, bucket_size=bucket, **({"cell": cell} if by_cell else {}),
                trials=len(subset),
                mean_trial_ownership_percent=statistics.fmean(r["ownership_percent"] for r in subset),
                minimum_trial_ownership_percent=min(r["ownership_percent"] for r in subset),
                maximum_trial_ownership_percent=max(r["ownership_percent"] for r in subset),
                trials_with_only_fully_owned_records=sum(
                    r["partly_or_not_owned_record_observations"] == 0 for r in subset),
                mean_trial_coverage_percent=statistics.fmean(r["coverage_percent"] for r in subset),
                minimum_trial_coverage_percent=min(r["coverage_percent"] for r in subset),
                storage_samples=sum(r["storage_samples"] for r in subset),
                complete_storage_samples=sum(r["complete_storage_samples"] for r in subset),
                evaluable_record_observations=sum(r["evaluable_record_observations"] for r in subset),
                fully_owned_record_observations=sum(r["fully_owned_record_observations"] for r in subset),
                partly_or_not_owned_record_observations=sum(
                    r["partly_or_not_owned_record_observations"] for r in subset),
            ))
    return result


def plot(rows, summaries, destination):
    os.environ.setdefault("MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "dit616-ownership-mpl"))
    cache = Path(tempfile.gettempdir()) / "dit616-ownership-cache"
    cache.mkdir(exist_ok=True)
    os.environ.setdefault("XDG_CACHE_HOME", str(cache))
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D
    import numpy as np

    # Computer Modern matches the report's default LaTeX/TikZ typography.
    plt.rcParams.update({"font.family": "serif", "font.serif": ["cmr10"], "font.size": 9,
                         "mathtext.fontset": "cm", "axes.formatter.use_mathtext": True,
                         "pdf.fonttype": 42, "ps.fonttype": 42, "svg.fonttype": "none"})
    fig, axis = plt.subplots(figsize=(7.4, 5.0))
    fig.subplots_adjust(left=.18, right=.975, bottom=.27, top=.73)
    colors = ("#527c96", "#bf603d")
    markers = ("o", "s")
    offsets = (-.15, .15)
    for index in range(8):
        if index % 2 == 0:
            axis.axvspan(index - .5, index + .5, color="#f2f3f4", zorder=0)
    for study_index, (name, _, _) in enumerate(STUDIES):
        for index, cell in enumerate(CELLS):
            subset = [r for r in rows if r["study"] == name and r["cell"] == cell]
            values = [r["ownership_percent"] for r in subset]
            x = index + offsets[study_index]
            axis.scatter(x + np.linspace(-.075, .075, len(values)), values,
                         s=15, marker=markers[study_index], color=colors[study_index],
                         alpha=.55, linewidths=0, zorder=3)
            axis.scatter([x], [statistics.fmean(values)], s=42, marker="D",
                         color=colors[study_index], edgecolor="#202020", linewidth=.6, zorder=4)
    axis.axhline(100, color="#5f6267", linewidth=.8, linestyle=(0, (3, 3)), zorder=1)
    axis.spines[["top", "right"]].set_visible(False)
    for spine in ("bottom", "left"):
        axis.spines[spine].set_color("#b7bbc0")
    axis.grid(axis="y", color="#d8dce0", linewidth=.55, zorder=0)
    axis.tick_params(axis="y", length=3, color="#8b9095")
    axis.set_xticks([])
    axis.set_xlim(-.5, 7.5)
    axis.set_ylim(98.5, 100.18)
    axis.set_yticks([98.5, 99, 99.5, 100], ["98.5", "99.0", "99.5", "100"])
    axis.set_ylabel("Attacker-owned replica holders (%)", labelpad=12)
    axis.set_axisbelow(True)

    # Aligned treatment matrix replaces encoded C/F/P tick labels.
    for row_index, label in enumerate(("Coordinated", "Filtering", "Poisoning")):
        y = -.10 - .11 * row_index
        axis.text(-.045, y, label, transform=axis.transAxes,
                  ha="right", va="center", fontsize=9, clip_on=False)
        for index, cell in enumerate(CELLS):
            enabled = cell.split("-")[row_index][1] == "1"
            axis.text(index, y, "On" if enabled else "Off",
                      transform=axis.get_xaxis_transform(), ha="center", va="center",
                      fontsize=9, color="#252a30", clip_on=False)

    fig.suptitle("Ownership of group-key replica holders", x=.18, y=.975, ha="left", fontsize=13)
    fig.text(.18, .89,
             "Mean trial ownership: " + "   |   ".join(
                 f"{r['study']}: {r['mean_trial_ownership_percent']:.2f}%" for r in summaries),
             fontsize=9, color="#252a30")
    handles = [Line2D([], [], color=color, marker=marker, linestyle="none", markersize=5,
                      label=f"{name} (bucket size {bucket})")
               for (name, bucket, _), color, marker in zip(STUDIES, colors, markers)]
    handles.append(Line2D([], [], color="#333333", marker="D", linestyle="none", markersize=5,
                          label="Mean across 10 trials"))
    fig.legend(handles=handles, loc="upper left", bbox_to_anchor=(.17, .84),
               frameon=False, ncol=3, fontsize=8, columnspacing=1.5, handletextpad=.35)
    fig.savefig(destination / "groupkey-replica-ownership.png", dpi=240, facecolor="white")
    plt.close(fig)
    return {"matplotlib": matplotlib.__version__, "numpy": np.__version__}


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input-root", type=Path, default=ROOT / "data")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    rows, hashes = [], {}
    for study in STUDIES:
        values, inputs = read_study(args.input_root, *study)
        rows.extend(values)
        hashes.update(inputs)
    summary = summarize(rows, by_cell=False)
    args.output_dir.mkdir(parents=True, exist_ok=True)
    versions = plot(rows, summary, args.output_dir)
    write_csv(args.output_dir / "ownership-by-trial.csv", rows)
    write_csv(args.output_dir / "ownership-by-condition.csv", summarize(rows, by_cell=True))
    write_csv(args.output_dir / "ownership-by-study.csv", summary)
    audit = dict(
        inputs=hashes, script_sha256=sha256(Path(__file__)), versions=versions,
        ownership_definition="Within-trial mean of attacker-owned replica-holder fractions over active announcement-record observations with complete snapshot coverage. Study/cell summaries weight trials equally; cache records excluded.",
        coverage_definition="complete_storage_samples / storage_samples per trial, from trials.csv. Storage CSV rows are record observations, not storage snapshot counts.",
        limitations=["Incomplete/stale snapshots are unknown, never imputed as fully owned.",
                     "No continuous-time ownership guarantee; full coverage is an asynchronous snapshot freshness check.",
                     "Configured num_replicas=5 does not imply five observed holders for every record.",
                     "All eight cells use targeted attacker placement; F=0 is not a no-placement baseline.",
                     "Repeated record observations are not independent trials.",
                     "SC 2 uses compact instrumentation and has five disclosed technical exclusions; this figure uses only its 80 valid mains."],
        studies=summary,
    )
    (args.output_dir / "ownership-audit.json").write_text(json.dumps(audit, indent=2) + "\n")
    print(json.dumps(dict(output=str(args.output_dir.resolve()), studies=summary), indent=2))


if __name__ == "__main__":
    main()
