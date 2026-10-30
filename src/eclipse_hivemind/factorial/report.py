"""Export validated campaign measurements, contrasts and overview figures."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import statistics
import tarfile
import tempfile

from . import study
from .evidence import analyse, mean
from .workflow import collapsed_rows, verify_completed, verify_trial_inputs


def csv_file(path, rows):
    if not rows:
        path.write_text("")
        return
    with path.open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def interval(values):
    import numpy as np
    values = [float(v) for v in values if v is not None]
    if not values:
        return None, None, None
    center = statistics.fmean(values)
    if len(values) < 2:
        return center, None, None
    rng = np.random.default_rng(20261018)
    samples = rng.choice(values, size=(5000, len(values)), replace=True).mean(axis=1)
    lo, hi = np.quantile(samples, (0.025, 0.975))
    return center, float(lo), float(hi)


def contrasts(trials):
    indexed = {(row["cell"], int(row["seed"])): row for row in trials if row["technical_valid"]}
    result = []
    metrics = ("victim_accuracy", "victim_loss", "candidate_share", "partner_share", "recruiter_leader_rate", "successful_capture_fraction")
    seeds = sorted({key[1] for key in indexed})
    for axis, name in enumerate(("coordination", "filtering", "poisoning")):
        for fixed in itertools.product((0, 1), repeat=2):
            off = list(fixed)
            off.insert(axis, 0)
            on = list(off)
            on[axis] = 1
            cells = [f"c{v[0]}-f{v[1]}-p{v[2]}" for v in (off, on)]
            for metric in metrics:
                values = []
                for seed in seeds:
                    a, b = (indexed.get((cell, seed), {}) for cell in cells)
                    if a.get(metric) is not None and b.get(metric) is not None:
                        values.append(float(b[metric]) - float(a[metric]))
                center, lo, hi = interval(values)
                result.append(dict(contrast=name + "_on_minus_off", off=cells[0], on=cells[1], metric=metric,
                                   paired_seeds=len(values), mean=center, ci_low=lo, ci_high=hi))
    # Difference of paired poisoning effects between C states at each F.
    for f in (0, 1):
        for metric in ("victim_accuracy", "victim_loss"):
            values = []
            for seed in seeds:
                cells = [f"c{c}-f{f}-p{p}" for c, p in ((0, 0), (0, 1), (1, 0), (1, 1))]
                v = [indexed.get((cell, seed), {}).get(metric) for cell in cells]
                if all(item is not None for item in v):
                    values.append((v[3] - v[2]) - (v[1] - v[0]))
            center, lo, hi = interval(values)
            result.append(dict(contrast="coordination_by_poisoning", off=f"c0-f{f}", on=f"c1-f{f}", metric=metric,
                               paired_seeds=len(values), mean=center, ci_low=lo, ci_high=hi))
    return result


def figures(destination, trials, evaluations, title_prefix=""):
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/dit616-factorial-matplotlib")
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    import numpy as np
    destination.mkdir(parents=True, exist_ok=True)
    valid = [row for row in trials if row["technical_valid"]]
    colors = ("#527c96", "#bf603d")
    labels = [f"C={c}, F={f}" for c, f in itertools.product((0, 1), repeat=2)]

    def bars(axis, metric, title, percent=False):
        for poison in (0, 1):
            for index, (c, f) in enumerate(itertools.product((0, 1), repeat=2)):
                subset = [row[metric] for row in valid if row["cell"] == f"c{c}-f{f}-p{poison}" and row.get(metric) is not None]
                scale = 100 if percent else 1
                values = [float(v) * scale for v in subset]
                center, lo, hi = interval(values)
                x = index + (-0.19 if poison == 0 else 0.19)
                axis.bar(x, np.nan if center is None else center, width=.34, color=colors[poison],
                         label="Standard gradients" if poison == 0 and index == 0 else "Reversed gradients" if index == 0 else None)
                axis.text(x, .98, f"n={len(values)}", transform=axis.get_xaxis_transform(), ha="center", va="top", fontsize=7)
                if center is not None:
                    if lo is not None:
                        axis.errorbar(x, center, yerr=[[max(0, center - lo)], [max(0, hi - center)]], color="#222222", capsize=3)
                    axis.scatter(x + np.linspace(-.08, .08, len(values)), values, color="#242424", s=12, alpha=.7)
                else:
                    axis.text(x, .02, "NA", transform=axis.get_xaxis_transform(), ha="center")
        axis.set_xticks(range(4), labels)
        axis.set_title(title)
        axis.set_ylabel("Percent" if percent else "Cross-entropy loss")
        if percent:
            axis.set_ylim(0, 105)
        axis.grid(axis="y", alpha=.2)
        axis.set_axisbelow(True)

    def save(fig, name):
        if title_prefix and fig._suptitle is not None:
            fig._suptitle.set_text(title_prefix + fig._suptitle.get_text())
        fig.savefig(destination / f"{name}.png", dpi=180)
        plt.close(fig)

    fig, axes = plt.subplots(1, 2, figsize=(12, 4.5), constrained_layout=True)
    bars(axes[0], "victim_accuracy", "Final Victim evaluation accuracy", True)
    bars(axes[1], "victim_loss", "Final Victim evaluation loss")
    fig.legend(*axes[0].get_legend_handles_labels(), loc="outside lower center", ncol=2, fontsize=8)
    fig.suptitle(f"Learning outcomes at optimizer epoch {study.ROUNDS}; dots = independent trials")
    save(fig, "01-learning-outcomes")
    fig, axes = plt.subplots(2, 2, figsize=(12, 8), constrained_layout=True)
    for axis, metric, title in zip(axes.flat, ("candidate_share", "partner_share", "recruiter_leader_rate", "successful_capture_fraction"),
                                 ("Recruiters among visible candidates", "Recruiters among formed-group partners",
                                  "Victim groups led by a recruiter", "Confirmed successful exact group captures")):
        bars(axis, metric, title, True)
    fig.legend(*axes.flat[0].get_legend_handles_labels(), loc="outside lower center", ncol=2, fontsize=8)
    fig.suptitle("Discovery → membership → leadership → successful capture")
    save(fig, "02-network-control")
    valid_ids = {row["run_id"] for row in valid}
    fig, axes = plt.subplots(2, 2, figsize=(12, 7), constrained_layout=True)
    for axis, (c, f) in zip(axes.flat, itertools.product((0, 1), repeat=2)):
        for poison in (0, 1):
            xs, centers, lows, highs = [], [], [], []
            for round_number in range(study.ROUNDS + 1):
                values = [r["loss"] for r in evaluations if r["run_id"] in valid_ids and r["node_id"] == "victim-0"
                          and r["cell"] == f"c{c}-f{f}-p{poison}" and r["round"] == round_number]
                center, lo, hi = interval(values)
                xs.append(round_number)
                centers.append(center if center is not None else np.nan)
                lows.append(lo if lo is not None else np.nan)
                highs.append(hi if hi is not None else np.nan)
            axis.plot(xs, centers, color=colors[poison], label=f"P={poison}")
            axis.fill_between(xs, lows, highs, color=colors[poison], alpha=.15)
        axis.set(title=f"Coordination={c}, filtering={f}", xlabel="Local optimizer epoch", ylabel="Victim evaluation loss")
        axis.grid(alpha=.2)
        axis.legend()
    fig.suptitle("Observed learning trajectories; 95% bootstrap intervals across trial means")
    save(fig, "03-learning-progress")


def render(root, trials, evaluations, title_prefix="", publish_report=True):
    data = root / "data"
    data.mkdir(parents=True, exist_ok=True)
    csv_file(data / "trials.csv", trials)
    effects = contrasts(trials)
    csv_file(data / "contrasts.csv", effects)
    figures(root / "figures", trials, evaluations, title_prefix)
    lines = ["# " + title_prefix + "Fixed-budget C × F × P experiment", "",
             f"Twenty trainers, five owners, three recruiter Averagers, {study.ROUNDS} local optimizer epochs; ten planned repetitions per cell.", "",
             "C: coordinated group-control bundle. F: DHT response filtering. P: gradient inversion on the same three recruiters.", "",
             "Dots are independent trial summaries; all trials have equal weight. Intervals are 95% percentile bootstrap intervals (5,000 seed resamples). Undefined values are not zeros.", "",
             "| Cell | Technical valid / observed | Complete endpoints | Victim accuracy | Victim loss | Successful capture / groups (trial mean) |",
             "| --- | --- | --- | --- | --- | --- |"]
    fmt = lambda v: "—" if v is None else f"{v:.4f}"
    for cell in study.CELLS:
        subset = [row for row in trials if row["cell"] == cell]
        valid = [row for row in subset if row["technical_valid"]]
        lines.append(f"| {cell} | {len(valid)}/{len(subset)} | {sum(row['endpoint_complete'] for row in valid)} | "
                     f"{fmt(mean([r['victim_accuracy'] for r in valid]))} | {fmt(mean([r['victim_loss'] for r in valid]))} | "
                     f"{fmt(mean([r['successful_capture_fraction'] for r in valid]))} |")
    lines += ["", "## Reading the measurements", "",
              "The three figures show final learning outcomes, network control, and loss trajectories. Exact capture means Victim plus all three recruiters; success additionally requires a confirmed Victim exchange. Unknown exchanges remain unknown.", "",
              "Candidate shares are snapshot-weighted discovery observations, excluding the Victim. Partner shares exclude the Victim: exact capture is 100% of its partners and 75% of the full group. The global recruiter fraction is 15%; all five owners constitute 25% of the swarm.", "",
              "Membership describes the formed group. A successful Victim exchange does not prove that every listed member started aggregation. Input-weight coverage, absent member observations and zero-total groups are recorded separately. Partial and zero-total weights have no defined relative weight share; absent weights are never replaced with zero. These coverage limits do not invalidate complete network and learning observations.", "",
              "Use data/trials.csv for time/group index to first successful capture, censored no-capture runs, consecutive-capture length, ordinary honest-node mean/worst learning outcomes, averaging success/progress, holder ownership/coverage and actual contribution weights. data/evaluations.csv preserves node-level curves and coverage; other detail CSVs preserve denominators.", "",
              "## Matched effects", "", "| Comparison | Metric | Paired seeds | Mean difference | 95% interval |",
              "| --- | --- | --- | --- | --- |"]
    for row in effects:
        if row["metric"] in {"victim_accuracy", "victim_loss"} and row["contrast"] in {"poisoning_on_minus_off", "coordination_by_poisoning"}:
            lines.append(f"| {row['contrast']}: {row['off']} → {row['on']} | {row['metric']} | {row['paired_seeds']} | {fmt(row['mean'])} | {fmt(row['ci_low'])} to {fmt(row['ci_high'])} |")
    lines += ["", "## Scope", "", "These are fixed-budget, local toy-task measurements. Observed groups do not certify continuous network isolation. The design does not establish a minimum attacker count, recovery, formal convergence speed or asymptotic divergence. A coordination-bundle interaction does not isolate its individual components or prove mediation by capture.", "",
              "Raw JSONL, SQLite, coordination audit and configuration artifacts remain in verified lossless archives; no historical dataset is pooled into these 80 trials."]
    text = "\n".join(lines) + "\n"
    if publish_report:
        (root / "REPORT.md").write_text(text)
    return text


def atomic_write(path, text):
    path = Path(path)
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, prefix=f".{path.name}.", delete=False) as stream:
        temporary = Path(stream.name)
        try:
            stream.write(text)
            stream.flush()
            os.fsync(stream.fileno())
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
    try:
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)


def export(root=study.RESULTS, title_prefix=""):
    root = Path(root)
    from . import failures
    attempts = [row for row in collapsed_rows(root) if row["phase"] == "main"]
    excluded = [row for row in attempts if row["status"] == failures.STATUS]
    rows = [row for row in attempts if row["status"] != failures.STATUS]
    expected = {(row["cell"], str(row["seed"])) for row in study.plan("main")}
    if len(rows) != 80 or {(row["cell"], row["seed"]) for row in rows} != expected or any(row["status"] != "ok" for row in rows):
        raise ValueError("final report requires all 80 unique archived primary trials; incomplete attempts stay in the manifest")
    if (root / "REPORT.md").exists():
        raise ValueError("report exists; choose a fresh output or inspect it before regenerating")
    protocol = json.loads((root / "protocol.json").read_text())
    for row in excluded:
        failures.verify_excluded(row, protocol)
    if protocol["rounds"] != study.ROUNDS or protocol["main_plan"] != study.plan("main"):
        raise ValueError("protocol is not the frozen 12-round factorial plan")
    indexed_plan = {(r["cell"], str(r["seed"])): r for r in protocol["main_plan"]}
    exclusion_counts = Counter((r["cell"], str(r["seed"])) for r in excluded)
    for row in excluded:
        key = row["cell"], str(row["seed"])
        if (key not in indexed_plan or row["config_sha256"] != indexed_plan[key]["config_sha256"]
                or exclusion_counts[key] > failures.MAX_REPLACEMENTS):
            raise ValueError("technical exclusions differ from the frozen plan or exceed their limit")
    data = root / "data"
    data.mkdir(exist_ok=True)
    streams, writers = {}, {}
    evaluations, trials = [], []

    def emit(table, row):
        if table not in writers:
            streams[table] = (data / f"{table}.csv").open("w", newline="")
            writers[table] = csv.DictWriter(streams[table], fieldnames=list(row))
            writers[table].writeheader()
        writers[table].writerow(row)
        if table == "evaluations":
            evaluations.append(row)

    try:
        for row in rows:
            metadata = verify_completed(row)
            if row["config_sha256"] != indexed_plan[(row["cell"], row["seed"])]["config_sha256"]:
                raise ValueError("manifest configuration changed")
            with tarfile.open(row["archive"], "r:gz") as archive:
                config_bytes = archive.extractfile("config.yaml").read()
                if hashlib.sha256(config_bytes).hexdigest() != row["config_sha256"]:
                    raise ValueError("archived configuration is not the planned cell")
                inputs = json.load(archive.extractfile("input-lock.json"))
                verify_trial_inputs(inputs, protocol, row)
                with archive.extractfile("events.jsonl") as stream:
                    result = analyse(stream, metadata["trial"], emit)
                if result != metadata["evidence"]:
                    raise ValueError("recomputed trial evidence disagrees with closed-run validation")
                trials.append(result)
    finally:
        for stream in streams.values():
            stream.close()
    text = render(root, trials, evaluations, title_prefix, publish_report=False)
    csv_file(data / "technical-exclusions.csv", excluded)
    if excluded:
        counts = {cell: sum(r["cell"] == cell for r in excluded) for cell in study.CELLS}
        text += ("\n## Technical exclusions\n\n"
                 f"{len(excluded)} additional attempts were excluded after completed training because native DHT "
                 "resource-sharer teardown or a terminal spool-drain race prevented a complete transport audit. These attempts are not counted "
                 "among the 80 validated measurements. The same cell and seed were repeated with identical node "
                 "images and settings; learning and attack outcomes did not select replacements. "
                 "The bounded automatic replacement policy is recorded in protocol.json before this rerun. It is not "
                 "a statistical correction for missing trials. Repetition does not recreate identical asynchronous scheduling. "
                 "Interpret results conditional on technical completion; differential failure rates can bias comparisons. "
                 "The exclusion reason for each attempt and all raw diagnostics are retained in data/technical-exclusions.csv and verified archives.\n\n"
                 "| Cell | Excluded attempts |\n| --- | --- |\n" +
                 "".join(f"| {cell} | {counts[cell]} |\n" for cell in study.CELLS))
    atomic_write(root / "analysis-audit.json", json.dumps(dict(
        complete=True, trials=80, synthetic=bool(title_prefix), sources=study.sources(),
        archives=[dict(run_id=r["run_id"], sha256=r["archive_sha256"]) for r in rows],
        technical_exclusions=[dict(run_id=r["run_id"], sha256=r["archive_sha256"]) for r in excluded]), indent=2) + "\n")
    # REPORT.md is the completion marker. Publish it only after data, figures
    # and the integrity audit are durable, so an interrupted export can retry.
    atomic_write(root / "REPORT.md", text)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=study.RESULTS)
    args = parser.parse_args()
    export(args.root)
