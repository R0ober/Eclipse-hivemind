import argparse
import sys

from .config import ConfigError, parse_config_file


def main() -> None:
    p = argparse.ArgumentParser(description="Validate a Hivemind experiment config.")
    p.add_argument("--config-path", type=str, required=True, help="Path to YAML file")
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
    print(f"{'Seed Node Type:':<20}{config.bootstrap.seeds.node_type}")

## HAND OF TO ORCHESTRATOR HERE 


if __name__ == "__main__":
    main()
