"""Build self-contained images, probe installed Hivemind and freeze a new campaign."""
from __future__ import annotations

import argparse
import importlib
import json
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))


def command(*args):
    subprocess.run(list(args), cwd=ROOT, check=True)


def prepare(name, build=True):
    package = "factorial" if name == "sc1" else "factorial2"
    study = importlib.import_module(f"eclipse_hivemind.{package}.study")
    workflow = importlib.import_module(f"eclipse_hivemind.{package}.workflow")
    study.prepare_configs()
    lock_path = study.RESULTS / "image-lock.json"
    if lock_path.exists():
        workflow.verify_images(workflow.image_lock())
        print(f"{name}: existing source/image lock verified; nothing rebuilt")
        return
    if build:
        for target, tag in (("logger", study.IMAGES["logger"]), ("coordinator", study.IMAGES["coordinator"])):
            command("docker", "build", "--target", target, "-f", "docker/services.Dockerfile", "-t", tag, ".")
        command("docker", "build", "--target", name, "-f", "docker/node.Dockerfile", "-t", study.IMAGE, ".")
    probe_name = "probe_factorial_image.py" if name == "sc1" else "probe_factorial_2_compact_image.py"
    output = subprocess.check_output([
        "docker", "run", "--rm", "--network", "none",
        "-v", f"{ROOT / 'checks'}:/review:ro", study.IMAGE, "python", f"/review/{probe_name}"
    ], cwd=ROOT, text=True)
    probe = json.loads(output)
    if probe.get("complete") is not True:
        raise ValueError("installed-library probe did not complete")
    for name_in_repo, digest in probe["embedded_sources"].items():
        if study.sha(ROOT / name_in_repo) != digest:
            raise ValueError(f"image source differs from checkout: {name_in_repo}")
    tags = list(study.IMAGES.values())
    inventory = json.loads(subprocess.check_output(["docker", "image", "inspect", *tags], text=True))
    images = {tag: item["Id"] for tag, item in zip(tags, inventory, strict=True)}
    study.immutable(lock_path, dict(images=images, sources=study.sources(), probe=probe))
    print(f"{name}: installed-library probe passed; image/source lock saved to {lock_path.relative_to(ROOT)}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("study", choices=("sc1", "sc2"))
    parser.add_argument("--no-build", action="store_true", help="Probe already-built publication images")
    args = parser.parse_args()
    prepare(args.study, build=not args.no_build)


if __name__ == "__main__":
    main()
