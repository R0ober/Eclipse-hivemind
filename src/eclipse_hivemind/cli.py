import argparse
import sys

from .config import ConfigError, parse_config_file
from orchestrator.docker_backend import DockerBackend
from orchestrator import orchestrator


def main() -> None:
    p = argparse.ArgumentParser(description="Validate a Hivemind experiment config.")
    p.add_argument("--config-path", type=str, required=True, help="Path to YAML file")
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

    try:
        with orchestrator.run(
            config,
            DockerBackend(),
            fake_nodes=args.fake_nodes,
        ) as (run_id, network_id, aggregator_id, nodes):
            # Machine-readable and on its own line: a sweep parses this to map a run
            # back to the config and seed that produced it.
            print(f"RUN_ID={run_id}", flush=True)
            print(f"{'Docker Network:':<20}{network_id}")
            print(f"{'Aggregator:':<20}{aggregator_id}")
            for node in nodes:
                role = "seed" if node["is_seed"] else "peer"
                print(f"{node['node_id']:<20}{role:<6}{node['maddr'] or ''}")
    except Exception as err:
        print(f"Docker Error: {err}")
        sys.exit(1)


if __name__ == "__main__":
    main()
