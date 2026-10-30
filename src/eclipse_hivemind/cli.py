import argparse
import re
import sys
from pathlib import Path

from .config import ConfigError, parse_config_file
from orchestrator.docker_backend import DockerBackend
from orchestrator import orchestrator


def parse_run_id(value: str) -> str:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,127}", value):
        raise argparse.ArgumentTypeError(
            "run-id must be 1–128 letters, numbers, underscores or hyphens"
        )
    return value


def main() -> None:
    p = argparse.ArgumentParser(description="Validate a Hivemind experiment config.")
    p.add_argument("--config-path", type=str, required=True, help="Path to YAML file")
    p.add_argument("--run-id", type=parse_run_id,
                   help="Explicit output identity; otherwise generate a unique run ID")
    p.add_argument("--output-root", type=Path,
                   help="Parent directory of raw run folders; defaults to ./outputs")
    p.add_argument("--capture-logs", action="store_true", help="Save owned container logs and exit state before removal")
    p.add_argument("--wait-for-exit", action="store_true", help="Wait for natural trainer exits before cleanup")
    p.add_argument(
        "--fake-nodes",
        action="store_true",
        help="Use Alpine test nodes that emit fake Hivemind addresses",
    )
    args = p.parse_args()

    try:
        config = parse_config_file(args.config_path)
    except ConfigError as err:
        print(f"Configuration Error: {err}")
        sys.exit(1)

    print("YAML configuration successfully loaded and validated!")
    print(f"{'Experiment Name:':<20}{config.experiment.name}")
    print(f"{'Total Nodes:':<20}{config.total_nodes()}")
    print(f"{'Active Nodes:':<20}{list(config.node_types_with_nodes().keys())}")
    for node_type, node_config in config.node_types.items():
        label = f"{node_type.title()} Nodes:"
        print(f"{label:<20}{node_config.count}")
    print(f"{'Experiment Seed:':<20}{config.experiment.seed}")
    print(f"{'Seed Node Type:':<20}{config.bootstrap.seeds.node_type}")

    run_id = args.run_id or orchestrator._new_run_id()
    # Emit the identity before Docker setup or training can fail.
    print(f"RUN_ID={run_id}", flush=True)
    try:
        with orchestrator.run(
            config,
            DockerBackend(),
            run_id=run_id,
            fake_nodes=args.fake_nodes,
            output_root=args.output_root,
            capture_logs=args.capture_logs,
            wait_for_exit=args.wait_for_exit,
        ) as (run_id, network_id, logger_id, nodes):
            print(f"{'Docker Network:':<20}{network_id}")
            print(f"{'Logger:':<20}{logger_id}")
            for node in nodes:
                role = "seed" if node["is_seed"] else "peer"
                print(f"{node['node_id']:<20}{role:<6}{node['maddr'] or ''}")
    except Exception as err:
        print(f"Docker Error: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
