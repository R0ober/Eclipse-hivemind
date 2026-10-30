"""Keep SC 2 staging, raw archives and measured release data separate."""
from __future__ import annotations

import json
from pathlib import Path
import re
import shutil

from . import study

RUN_ID = re.compile(r"factorial2-(pilot|main)-c[01]-f[01]-p[01]-s[0-9]+-[0-9a-f]{10}")


def no_symlinks(path):
    path = Path(path).absolute()
    if ".." in path.parts or any(p.is_symlink() for p in (path, *path.parents)):
        raise ValueError(f"refusing symbolic link or parent traversal: {path}")
    return path


def no_linked_descendants(directory):
    no_symlinks(directory)
    for path in Path(directory).rglob("*"):
        if path.is_symlink():
            raise ValueError(f"refusing linked campaign content: {path}")


def validate_layout():
    for path in (study.CONFIGS, study.RESULTS, study.OPERATIONS, study.ARCHIVE):
        no_symlinks(path)
    for name in ("src", "configs", "data", "scripts", "checks", "docker", "outputs"):
        protected = study.ROOT / name
        if study.ARCHIVE == protected or study.ARCHIVE.is_relative_to(protected):
            raise ValueError("archive destination overlaps source, data or outputs")


def prepare_local():
    validate_layout()
    for path in (study.RESULTS, study.OPERATIONS):
        path.mkdir(parents=True, exist_ok=True)
        no_linked_descendants(path)


def prepare_archive(archive_root):
    validate_layout()
    if Path(archive_root).absolute() != study.ARCHIVE.absolute():
        raise ValueError("archive destination differs from the selected campaign")
    no_symlinks(archive_root)
    archive_root.parent.mkdir(parents=True, exist_ok=True)


def validate_trial_paths(directory, archive_root, trial):
    validate_layout()
    if not RUN_ID.fullmatch(trial["run_id"]):
        raise ValueError("unrecognized SC 2 run identity")
    if (Path(directory).absolute() != (study.OPERATIONS / "staging" / trial["run_id"]).absolute()
            or Path(archive_root).absolute() != study.ARCHIVE.absolute()):
        raise ValueError("raw path differs from this trial's staging or archive")
    no_symlinks(directory)
    no_symlinks(archive_root)


def remove_verified_stage(directory, trial, target, sidecar):
    """Remove only a new staged duplicate whose complete archive is verified."""
    from .workflow import raw_inventory, verify_archive
    validate_trial_paths(directory, study.ARCHIVE, trial)
    expected = study.ARCHIVE / (trial["run_id"] + ".tar.gz")
    if Path(target).absolute() != expected.absolute() or Path(sidecar).absolute() != expected.with_suffix(".json").absolute():
        raise ValueError("cleanup archive identity differs")
    no_symlinks(target)
    no_symlinks(sidecar)
    metadata = json.loads(Path(sidecar).read_text())
    if metadata["trial"] != trial or raw_inventory(directory) != metadata["members"]:
        raise ValueError("staged files differ from their verified archive")
    verify_archive(target, metadata)
    shutil.rmtree(directory)
