"""Create and clean up the Docker network for a validated experiment."""

from __future__ import annotations

import contextlib
import datetime as _dt
import secrets
import time
from collections.abc import Iterator

from eclipse_hivemind.config import ExperimentConfig
from .backend import ContainerBackend
from .env import build_node_env 

AGGREGATOR_IMAGE = "alpine:3.20"
AGGREGATOR_COMMAND = ["sleep", "infinity"]


def _new_run_id() -> str:
    timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"run-{timestamp}-{secrets.token_hex(3)}"


def _start_node_type(
    config: ExperimentConfig,
    backend: ContainerBackend,
    node_type_name: str,
    run_id: str,
    network_id: str,
    labels: dict[str, str],
    created_containers: list[str],
) -> None:
    node_type_config = config.node_types[node_type_name]

    for node_index in range(node_type_config.count):
        node_env = build_node_env(
            config=config,
            run_id=run_id,
            node_type_name=node_type_name,
            node_index=node_index,
            initial_peers=[],
        )
        container_id = backend.create_container(
            image=node_type_config.image,
            name=f"{run_id}-{node_type_name}-{node_index}",
            network=network_id,
            env=node_env,
            labels=labels,
            command=AGGREGATOR_COMMAND,
        )
        created_containers.append(container_id)
        backend.start(container_id)


@contextlib.contextmanager
def run(
    config: ExperimentConfig,
    backend: ContainerBackend,
    run_id: str | None = None,
) -> Iterator[tuple[str, str]]:
    """Create the run network, yield its ID, and remove it on exit."""
    run_id = run_id or _new_run_id()
    network_name = f"eclipse-{run_id}"
    labels = {"eclipse_run": run_id}
    network_id = backend.create_network(network_name, labels)
    created_containers: list[str] = []

    try:
        aggregator_id = backend.create_container(
            image=AGGREGATOR_IMAGE,
            name=f"{run_id}-aggregator",
            network=network_id,
            env={"EXPERIMENT_ID": run_id},
            labels=labels,
            command=AGGREGATOR_COMMAND,
        )
        created_containers.append(aggregator_id)
        backend.start(aggregator_id)
        phases = config.startup.phases
        if not phases:
            for node_type_name in config.node_types:
                _start_node_type(
                    config,
                    backend,
                    node_type_name,
                    run_id,
                    network_id,
                    labels,
                    created_containers,
                )
        else:
            for phase in phases:
                for node_type_name in phase.node_types:
                    _start_node_type(
                        config,
                        backend,
                        node_type_name,
                        run_id,
                        network_id,
                        labels,
                        created_containers,
                    )
                if phase.wait_after_seconds:
                    time.sleep(phase.wait_after_seconds)

        yield network_id, aggregator_id
    finally:
        for container_id in reversed(created_containers):
            with contextlib.suppress(Exception):
                backend.stop(container_id)
            with contextlib.suppress(Exception):
                backend.remove_container(container_id)
        with contextlib.suppress(Exception):
            backend.remove_network(network_id)



