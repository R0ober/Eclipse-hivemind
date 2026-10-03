"""Create and clean up the Docker network for a validated experiment."""

from __future__ import annotations

import contextlib
import datetime as _dt
import json
import secrets
import time
from collections.abc import Iterator
from pathlib import Path

from eclipse_hivemind.config import ExperimentConfig
from eclipse_hivemind.logger.models import ExperimentRegistration, ExperimentNode
from .backend import ContainerBackend
from .env import build_node_env 

LOGGER_IMAGE = "eclipse-hivemind-logger:dev"
LOGGER_COMMAND = ["python", "-m", "eclipse_hivemind.logger"]
FAKE_NODE_IMAGE = "alpine:3.20"


def fake_node_command(node_id: str) -> list[str]:
    """Command for exercising node readiness before the Hivemind image exists."""
    address = f"/ip4/172.18.0.2/tcp/4000/p2p/fake-{node_id}"
    return [
        "sh",
        "-c",
        f"echo HIVEMIND_MADDR={address}; sleep infinity",
    ]


def _new_run_id() -> str:
    timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"run-{timestamp}-{secrets.token_hex(3)}"


def validate_runtime_config(config: ExperimentConfig) -> None:
    """Reject configuration modes the current orchestrator cannot execute."""
    if config.bootstrap.policy.value == "full":
        raise ValueError(
            "bootstrap.policy='full' is not implemented; use 'seed_only'"
        )


def select_seed_node_ids(config: ExperimentConfig) -> list[str]:
    """Return the configured node IDs that should act as bootstrap seeds."""
    seed_config = config.bootstrap.seeds
    return [
        f"{seed_config.node_type}-{index}"
        for index in range(seed_config.count)
    ]


def start_groups(config: ExperimentConfig) -> dict[str, str]:
    """Map each node type to the cohort it starts with.

    Nodes wait at the logger's start barrier for their own cohort, so a phase
    that starts later still joins a swarm that is already training - which is the
    point of startup.phases (ADR 0009). Without phases every node starts at once
    and the whole experiment is one cohort.
    """
    if not config.startup.phases:
        return {node_type_name: "all" for node_type_name in config.node_types}

    groups: dict[str, str] = {}
    for phase in config.startup.phases:
        for node_type_name in phase.node_types:
            groups.setdefault(node_type_name, phase.name)
    return groups


def build_experiment_manifest(
    config: ExperimentConfig,
    run_id: str,
) -> str:
    groups = start_groups(config)
    nodes = [
        ExperimentNode(
            node_id=f"{node_type_name}-{node_index}",
            node_type=node_type_name,
            node_index=node_index,
            start_group=groups[node_type_name],
        )
        for node_type_name, node_type in config.node_types.items()
        for node_index in range(node_type.count)
    ]
    return ExperimentRegistration(
        experiment_id=run_id,
        experiment_name=config.experiment.name,
        rounds=config.experiment.rounds,
        nodes=nodes,
    ).model_dump_json()


def extract_multiaddress(log_line: str) -> str:
    """Extract a Hivemind multiaddress from a readiness log line."""
    marker = "HIVEMIND_MADDR="
    if marker not in log_line:
        raise ValueError(f"log line does not contain {marker}")

    multiaddress = log_line.split(marker, 1)[1].strip().split()[0]
    if not multiaddress:
        raise ValueError("HIVEMIND_MADDR marker has no address")
    return multiaddress


def start_seed_nodes(
    config: ExperimentConfig,
    backend: ContainerBackend,
    run_id: str,
    network_id: str,
    labels: dict[str, str],
    created_containers: list[str],
    nodes: list[dict],
    readiness_timeout: float = 120,
    fake_nodes: bool = False,
) -> dict[str, str]:
    """Start configured seed nodes and return their Hivemind addresses."""
    seed_addresses: dict[str, str] = {}

    for node_id in select_seed_node_ids(config):
        node_type_name, index_text = node_id.rsplit("-", 1)
        node_index = int(index_text)
        node_type_config = config.node_types[node_type_name]
        node_env = build_node_env(
            config=config,
            run_id=run_id,
            node_type_name=node_type_name,
            node_index=node_index,
            initial_peers=list(seed_addresses.values()),
        )
        container_name = f"{run_id}-{node_id}"
        container_id = backend.create_container(
            image=FAKE_NODE_IMAGE if fake_nodes else node_type_config.image,
            name=container_name,
            network=network_id,
            env=node_env,
            labels=labels,
            command=fake_node_command(node_id) if fake_nodes else None,
        )
        created_containers.append(container_id)
        backend.start(container_id)
        log_line = backend.wait_for_log(
            container_id,
            "HIVEMIND_MADDR=",
            readiness_timeout,
        )
        maddr = extract_multiaddress(log_line)
        seed_addresses[node_id] = maddr
        nodes.append({
            "node_id": node_id,
            "node_type": node_type_name,
            "index": node_index,
            "container_id": container_id,
            "container_name": container_name,
            "maddr": maddr,
            "is_seed": True,
        })

    return seed_addresses


def _start_node_type(
    config: ExperimentConfig,
    backend: ContainerBackend,
    node_type_name: str,
    run_id: str,
    network_id: str,
    labels: dict[str, str],
    created_containers: list[str],
    initial_peers: list[str],
    skip_node_ids: set[str],
    fake_nodes: bool,
    nodes: list[dict],
) -> None:
    node_type_config = config.node_types[node_type_name]

    for node_index in range(node_type_config.count):
        node_id = f"{node_type_name}-{node_index}"
        if node_id in skip_node_ids:
            continue

        node_env = build_node_env(
            config=config,
            run_id=run_id,
            node_type_name=node_type_name,
            node_index=node_index,
            initial_peers=initial_peers,
        )
        container_name = f"{run_id}-{node_id}"
        container_id = backend.create_container(
            image=FAKE_NODE_IMAGE if fake_nodes else node_type_config.image,
            name=container_name,
            network=network_id,
            env=node_env,
            labels=labels,
            command=fake_node_command(node_id) if fake_nodes else None,
        )
        created_containers.append(container_id)
        backend.start(container_id)
        nodes.append({
            "node_id": node_id,
            "node_type": node_type_name,
            "index": node_index,
            "container_id": container_id,
            "container_name": container_name,
            "maddr": None,
            "is_seed": False,
        })


@contextlib.contextmanager
def run(
    config: ExperimentConfig,
    backend: ContainerBackend,
    run_id: str | None = None,
    fake_nodes: bool = False,
    outputs: Path = Path("outputs"),
) -> Iterator[tuple[str, str, str, list[dict]]]:
    """Create the run network, yield it with the started nodes, and clean up on exit.

    Announce the run ID before launching containers, and save the resolved config
    under outputs/run_id. Wait for each node's generic completion marker, then
    yield the run resources. On success or failure, preserve container logs before
    removal so the experiment can be diagnosed after cleanup.
    """

    validate_runtime_config(config)
    run_id = run_id or _new_run_id()
    network_name = f"eclipse-{run_id}"
    labels = {"eclipse_run": run_id}
    output_directory = outputs.resolve() / run_id
    output_directory.mkdir(parents=True, exist_ok=True)
    (output_directory / "config.json").write_text(config.model_dump_json(indent=2), encoding="utf-8")
    # Announce before launching containers so failures still map to their outputs.
    print(f"RUN_ID={run_id}", flush=True)
    network_id = backend.create_network(network_name, labels)
    created_containers: list[str] = []
    nodes: list[dict] = []

    try:
        logger_id = backend.create_container(
            image=LOGGER_IMAGE,
            name=f"{run_id}-logger",
            network=network_id,
            env={
                "EXPERIMENT_ID": run_id,
                "EXPERIMENT_MANIFEST": build_experiment_manifest(config, run_id),
                "EVENT_EXPORT_PATH": "/outputs/events.jsonl",
            },
            labels=labels,
            volumes={str(output_directory): {"bind": "/outputs", "mode": "rw"}},
            command=LOGGER_COMMAND,
            network_aliases=["logger"],
        )
        created_containers.append(logger_id)
        backend.start(logger_id)

        seed_addresses = start_seed_nodes(
            config=config,
            backend=backend,
            run_id=run_id,
            network_id=network_id,
            labels=labels,
            created_containers=created_containers,
            nodes=nodes,
            fake_nodes=fake_nodes,
        )
        seed_node_ids = set(seed_addresses)
        initial_peers = list(seed_addresses.values())

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
                    initial_peers,
                    seed_node_ids,
                    fake_nodes,
                    nodes,
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
                        initial_peers,
                        seed_node_ids,
                        fake_nodes,
                        nodes,
                    )
                if phase.wait_after_seconds:
                    time.sleep(phase.wait_after_seconds)

        if not fake_nodes:
            for node in nodes:
                backend.wait_for_log(
                    node["container_id"],
                    "NODE_COMPLETE=1",
                    timeout=config.experiment.node_timeout_seconds,
                )

        yield run_id, network_id, logger_id, nodes
    finally:
        for container_id in reversed(created_containers):
            with contextlib.suppress(Exception):
                backend.stop(container_id)
            try:
                logs = backend.container_logs(container_id)
                (output_directory / f"{container_id}.log").write_text(logs, encoding="utf-8")
            except Exception as error:
                # Keep cleaning up, but make missing diagnostics visible.
                print(f"Could not save logs for {container_id}: {error}", flush=True)
            with contextlib.suppress(Exception):
                backend.remove_container(container_id)
        with contextlib.suppress(Exception):
            backend.remove_network(network_id)



