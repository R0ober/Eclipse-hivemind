from __future__ import annotations
import hashlib
import itertools
import json
from pathlib import Path
import random
import yaml
from eclipse_hivemind.config import parse_config

ROOT = Path(__file__).resolve().parents[3]
NAME = "groupkey-eclipse-factorial-2"
VERSION = "sc2-publication"
ROUNDS = 12
IMAGE = "eclipse-hivemind-sc2:publication"
BUCKET_SIZE = 5
IMAGES = {"node": IMAGE, "logger": "eclipse-hivemind-logger:publication", "coordinator": "eclipse-hivemind-coordinator:publication"}
CONFIGS = ROOT / "configs/sc2"
RESULTS = ROOT / "outputs/sc2"
OPERATIONS = ROOT / "outputs/sc2-operations"
ARCHIVE = ROOT / "archives/sc2/raw"
CELLS = tuple(f"c{c}-f{f}-p{p}" for c, f, p in itertools.product((0, 1), repeat=3))

def factors(cell):
    if cell not in CELLS:
        raise ValueError(f"unknown condition: {cell}")
    return tuple(int(part[1]) for part in cell.split("-"))

def sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()

def immutable(path, value):
    from .safety import no_symlinks
    path = Path(path)
    no_symlinks(path)
    if path.exists():
        if json.loads(path.read_text()) != value:
            raise ValueError(f"immutable record changed: {path}")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("x") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")

def config(cell, seed):
    c, f, p = factors(cell)
    nodes = {}
    for role, count in (("normal", 14), ("victim", 1), ("recruiter", 3), ("support", 2)):
        owner = role in {"recruiter", "support"}
        parameters = dict(factorial_role=role, factor_coordination=bool(c), factor_filtering=bool(f),
                          factor_poisoning=bool(p), focal_victim=role == "victim",
                          storage_behavior="filter" if owner and f else "standard",
                          averager_behavior="reverse-gradient" if role == "recruiter" and p else "standard",
                          averager_role=("malicious" if p else "controlled") if role == "recruiter" else "honest",
                          dht_placement="targeted" if owner else "random",
                          target_batch_size=1280, batch_size=32, learning_rate=0.05, eval_size=4096,
                          matchmaking_time=10, averaging_timeout=30, step_delay_seconds=1,
                          target_group_size=4, min_group_size=2, num_replicas=5, bucket_size=BUCKET_SIZE,
                          storage_poll_seconds=1, start_barrier_timeout=180)
        if owner:
            parameters.update(dht_id_offset=1 if role == "recruiter" else 4, coordination_role=role,
                              coordinator_endpoint="http://attack-coordinator:8090")
        nodes[role] = dict(count=count, image=IMAGE, parameters=parameters)
    result = dict(schema_version=1, experiment=dict(name=NAME, seed=seed, rounds=ROUNDS),
                  logger=dict(endpoint="http://logger:8080", image=IMAGES["logger"]),
                  services={"attack-coordinator": dict(image=IMAGES["coordinator"], ready_log="ATTACK_COORDINATOR_READY=1",
                            environment={"COORDINATION_DB_PATH": "/outputs/coordination.sqlite3"})},
                  bootstrap=dict(seeds=dict(node_type="normal", count=1), policy="seed_only"), node_types=nodes)
    parse_config(yaml.safe_dump(result))
    return result

def plan(phase):
    if phase not in {"pilot", "main"}:
        raise ValueError("phase must be pilot or main")
    seeds = [10017] if phase == "pilot" else list(range(17, 27))
    result = []
    rng = random.Random(20261018)
    for seed in seeds:
        order = list(CELLS)
        rng.shuffle(order)
        for cell in order:
            path = CONFIGS / phase / f"{cell}-seed-{seed}.yaml"
            content = yaml.safe_dump(config(cell, seed), sort_keys=False)
            result.append(dict(cell=cell, seed=seed, config=str(path),
                               config_sha256=hashlib.sha256(content.encode()).hexdigest()))
    return result

def prepare_configs():
    from .safety import no_symlinks
    for phase in ("pilot", "main"):
        for row in plan(phase):
            path = Path(row["config"])
            no_symlinks(path)
            content = yaml.safe_dump(config(row["cell"], row["seed"]), sort_keys=False)
            if path.exists():
                if path.read_text() != content:
                    raise ValueError(f"frozen config changed: {path}")
            else:
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text(content)

def sources():
    paths = set()
    for folder in ("src", "scripts", "checks", "docker"):
        paths.update(p for p in (ROOT / folder).rglob("*")
                     if p.is_file() and "__pycache__" not in p.parts)
    paths.update(ROOT / name for name in ("pyproject.toml", "requirements.txt", ".dockerignore"))
    return {str(p.relative_to(ROOT)): sha(p) for p in sorted(paths)}

def protocol():
    return dict(name=NAME, version=VERSION, rounds=ROUNDS, training_nodes=20,
                attacker_owners=5, selected_recruiters=3, replicas=5, bucket_size=BUCKET_SIZE,
                main_trials=80, pilot_trials=8, common_image=IMAGE,
                factors=["coordination_bundle", "response_filtering", "gradient_inversion"],
                placement="targeted for the same five owners in all cells",
                sources=sources(), main_plan=plan("main"), pilot_plan=plan("pilot"),
                replacement_policy="At most two audited technical shutdown exclusions per main cell/seed; all other failures stop. No historical trials are imported.",
                validity="Measurement integrity is distinct from attack outcome.",
                scope="Fixed-budget local toy task; no minimum-count or continuous-isolation claim.")
