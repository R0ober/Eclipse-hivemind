import pytest

from eclipse_hivemind.config import ExperimentConfig, parse_config
from orchestrator import orchestrator

BASE = """
schema_version: 1
experiment:
  name: run-test
  seed: 1
  rounds: 2
aggregator:
  endpoint: http://aggregator:8080
bootstrap:
  seeds:
    node_type: normal
    count: {seed_count}
  policy: {policy}
node_types:
  normal:
    count: 3
    image: honest:dev
  adversarial:
    count: 1
    image: adversary:dev
"""

PHASES = """
startup:
  phases:
    - name: honest-network
      node_types: [normal]
      wait_after_seconds: 30
    - name: adversarial-nodes
      node_types: [adversarial]
"""


class FakeBackend:
    """Records what the orchestrator asked for instead of talking to Docker."""

    def __init__(self, fail_on_start: str | None = None) -> None:
        self.fail_on_start = fail_on_start
        self.networks: list[str] = []
        self.removed_networks: list[str] = []
        self.containers: dict[str, dict] = {}
        self.started: list[str] = []
        self.stopped: list[str] = []
        self.removed: list[str] = []
        self.waited: list[tuple[str, str]] = []

    def create_network(self, name: str, labels: dict[str, str]) -> str:
        self.networks.append(name)
        return f"id-of-{name}"

    def remove_network(self, name: str) -> None:
        self.removed_networks.append(name)

    def create_container(self, **container) -> str:
        self.containers[container["name"]] = container
        return container["name"]

    def start(self, container_id: str) -> None:
        if container_id == self.fail_on_start:
            raise RuntimeError(f"could not start {container_id}")
        self.started.append(container_id)

    def wait_for_log(self, container_id: str, pattern: str, timeout: float) -> str:
        self.waited.append((container_id, pattern))
        if pattern == "HIVEMIND_MADDR=":
            return f"HIVEMIND_MADDR={maddr(container_id)}"
        return pattern

    def stop(self, container_id: str) -> None:
        self.stopped.append(container_id)

    def remove_container(self, container_id: str) -> None:
        self.removed.append(container_id)


def maddr(container_id: str) -> str:
    return f"/ip4/10.0.0.1/tcp/1337/p2p/{container_id}"


def config(seed_count: int = 1, policy: str = "seed_only", extra: str = "") -> ExperimentConfig:
    return parse_config(BASE.format(seed_count=seed_count, policy=policy) + extra)


@pytest.fixture(autouse=True)
def isolated_run(tmp_path, monkeypatch) -> list[float]:
    """Keep outputs/ out of the repo and make phase waits instant."""
    sleeps: list[float] = []
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(orchestrator.time, "sleep", sleeps.append)
    return sleeps


def run(backend: FakeBackend, experiment: ExperimentConfig, **options) -> list[dict]:
    with orchestrator.run(experiment, backend, run_id="run-1", **options) as (_run, _network, _aggregator, nodes):
        return nodes


def test_aggregator_starts_first_and_is_given_the_manifest() -> None:
    backend = FakeBackend()

    run(backend, config())

    assert backend.started[0] == "run-1-aggregator"
    aggregator = backend.containers["run-1-aggregator"]
    assert aggregator["network_aliases"] == ["aggregator"]
    assert aggregator["env"]["EXPERIMENT_ID"] == "run-1"
    assert aggregator["env"]["EXPERIMENT_MANIFEST"] == orchestrator.build_experiment_manifest(config(), "run-1")


def test_seed_starts_before_the_other_nodes_and_they_get_its_address() -> None:
    backend = FakeBackend()

    run(backend, config())

    assert backend.started == [
        "run-1-aggregator",
        "run-1-normal-0",
        "run-1-normal-1",
        "run-1-normal-2",
        "run-1-adversarial-0",
    ]
    assert backend.containers["run-1-normal-0"]["env"]["INITIAL_PEERS"] == ""
    for name in ["run-1-normal-1", "run-1-normal-2", "run-1-adversarial-0"]:
        assert backend.containers[name]["env"]["INITIAL_PEERS"] == maddr("run-1-normal-0")


def test_a_later_seed_bootstraps_from_the_earlier_ones() -> None:
    backend = FakeBackend()

    run(backend, config(seed_count=2))

    both_seeds = f"{maddr('run-1-normal-0')} {maddr('run-1-normal-1')}"
    assert backend.containers["run-1-normal-1"]["env"]["INITIAL_PEERS"] == maddr("run-1-normal-0")
    assert backend.containers["run-1-normal-2"]["env"]["INITIAL_PEERS"] == both_seeds


def test_every_node_is_started_exactly_once() -> None:
    backend = FakeBackend()

    run(backend, config(seed_count=2))

    assert sorted(backend.started) == sorted(set(backend.started))
    assert len(backend.started) == 1 + config().total_nodes()


def test_each_node_runs_the_image_of_its_node_type() -> None:
    backend = FakeBackend()

    run(backend, config())

    assert backend.containers["run-1-normal-1"]["image"] == "honest:dev"
    assert backend.containers["run-1-adversarial-0"]["image"] == "adversary:dev"
    assert backend.containers["run-1-adversarial-0"]["command"] is None


def test_reported_nodes_say_which_are_seeds() -> None:
    nodes = run(FakeBackend(), config())

    assert [(node["node_id"], node["is_seed"], node["maddr"]) for node in nodes] == [
        ("normal-0", True, maddr("run-1-normal-0")),
        ("normal-1", False, None),
        ("normal-2", False, None),
        ("adversarial-0", False, None),
    ]


def test_run_waits_for_every_node_to_finish_training() -> None:
    backend = FakeBackend()

    nodes = run(backend, config())

    finished = [container for container, pattern in backend.waited if pattern == "TRAINING_COMPLETE=1"]
    assert finished == [node["container_id"] for node in nodes]


def test_fake_nodes_use_the_stand_in_image_and_are_not_waited_on() -> None:
    backend = FakeBackend()

    run(backend, config(), fake_nodes=True)

    node = backend.containers["run-1-normal-1"]
    assert node["image"] == orchestrator.FAKE_NODE_IMAGE
    assert node["command"] == orchestrator.fake_node_command("normal-1")
    assert not [pattern for _container, pattern in backend.waited if pattern == "TRAINING_COMPLETE=1"]


def test_phases_start_in_order_and_wait_between(isolated_run: list[float]) -> None:
    backend = FakeBackend()

    run(backend, config(extra=PHASES))

    assert backend.started[-1] == "run-1-adversarial-0"
    assert isolated_run == [30]


def test_node_type_in_no_phase_is_not_started() -> None:
    backend = FakeBackend()
    only_honest = """
startup:
  phases:
    - name: honest-network
      node_types: [normal]
"""

    run(backend, config(extra=only_honest))

    assert "run-1-adversarial-0" not in backend.containers


def test_everything_is_removed_when_the_run_ends() -> None:
    backend = FakeBackend()

    run(backend, config())

    assert sorted(backend.removed) == sorted(backend.containers)
    assert sorted(backend.stopped) == sorted(backend.containers)
    assert backend.removed_networks == ["id-of-eclipse-run-1"]


def test_everything_is_removed_when_a_node_fails_to_start() -> None:
    backend = FakeBackend(fail_on_start="run-1-normal-2")

    with pytest.raises(RuntimeError, match="could not start run-1-normal-2"):
        run(backend, config())

    assert sorted(backend.removed) == sorted(backend.containers)
    assert backend.removed_networks == ["id-of-eclipse-run-1"]


def test_unimplemented_bootstrap_policy_is_rejected_before_anything_is_created() -> None:
    backend = FakeBackend()

    with pytest.raises(ValueError, match="not implemented"):
        run(backend, config(policy="full"))

    assert backend.networks == []


def test_multiaddress_is_read_from_a_readiness_line() -> None:
    line = "2026-09-30 12:00:00 HIVEMIND_MADDR=/ip4/172.18.0.2/tcp/1337/p2p/Qm123 trailing"

    assert orchestrator.extract_multiaddress(line) == "/ip4/172.18.0.2/tcp/1337/p2p/Qm123"


def test_a_line_without_the_marker_is_not_a_multiaddress() -> None:
    with pytest.raises(ValueError):
        orchestrator.extract_multiaddress("HIVEMIND_READY=1 NODE_ID=normal-0")
