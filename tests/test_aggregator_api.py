from fastapi.testclient import TestClient

import json

import pytest

from eclipse_hivemind.aggregator.app import create_app
from eclipse_hivemind.aggregator.models import ExperimentRegistration
from eclipse_hivemind.aggregator.store import ExperimentAlreadyExists, InMemoryEventStore


def registration(nodes: list[dict] | None = None) -> dict:
    return {
        "experiment_id": "run-test",
        "experiment_name": "test-experiment",
        "rounds": 2,
        "nodes": nodes
        or [
            {"node_id": "normal-0", "node_type": "normal", "node_index": 0},
        ],
    }


def registered_app() -> TestClient:
    return TestClient(
        create_app(initial_registration=ExperimentRegistration.model_validate(registration()))
    )


def event_batch() -> dict:
    return {
        "experiment_id": "run-test",
        "node": {"node_id": "normal-0", "node_type": "normal", "node_index": 0},
        "events": [
            {
                "event_id": "event-1",
                "sequence": 0,
                "event_type": "node_started",
                "occurred_at": "2026-09-18T12:00:00Z",
                "data": {"runtime": "test"},
            }
        ],
    }


def test_register_and_submit_event() -> None:
    client = registered_app()

    response = client.post("/api/v1/experiments/run-test/events", json=event_batch())
    assert response.status_code == 202
    assert response.json() == {
        "accepted": 1,
        "duplicates": 0,
        "rejected": 0,
        "last_sequence": 0,
    }


def test_duplicate_event_is_acknowledged_without_duplication() -> None:
    client = registered_app()
    client.post("/api/v1/experiments/run-test/events", json=event_batch())

    response = client.post("/api/v1/experiments/run-test/events", json=event_batch())

    assert response.status_code == 202
    assert response.json()["accepted"] == 0
    assert response.json()["duplicates"] == 1

    response = client.get("/api/v1/experiments/run-test/summary")
    assert response.json() == {
        "experiment_id": "run-test",
        "event_count": 1,
        "node_count": 1,
    }


def test_unknown_node_is_rejected() -> None:
    client = registered_app()
    batch = event_batch()
    batch["node"]["node_id"] = "adversarial-0"

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.status_code == 404
    assert response.json()["detail"] == "unknown node"


def test_invalid_event_type_is_rejected() -> None:
    client = registered_app()
    batch = event_batch()
    batch["events"][0]["event_type"] = "made_up_event"

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.status_code == 422


def test_accepted_events_are_exported_as_json_lines(tmp_path) -> None:
    export_path = tmp_path / "events.jsonl"
    client = TestClient(
        create_app(
            store=InMemoryEventStore(export_path=export_path),
            initial_registration=ExperimentRegistration.model_validate(registration()),
        )
    )

    response = client.post("/api/v1/experiments/run-test/events", json=event_batch())

    assert response.status_code == 202
    assert [json.loads(line) for line in export_path.read_text().splitlines()] == [
        {
            "event": {
                "event_type": "node_started",
                "event_id": "event-1",
                "sequence": 0,
                "occurred_at": "2026-09-18T12:00:00+00:00",
                "data": {"runtime": "test"},
            },
            "node": {
                "node_id": "normal-0",
                "node_type": "normal",
                "node_index": 0,
            },
            "experiment_id": "run-test",
        }
    ]


def two_node_registration() -> dict:
    return registration(
        [
            {"node_id": "normal-0", "node_type": "normal", "node_index": 0},
            {"node_id": "normal-1", "node_type": "normal", "node_index": 1},
        ]
    )


def staged_registration() -> dict:
    """Two cohorts, as a startup plan with two phases produces."""
    return registration(
        [
            {
                "node_id": "normal-0",
                "node_type": "normal",
                "node_index": 0,
                "start_group": "honest-network",
            },
            {
                "node_id": "normal-1",
                "node_type": "normal",
                "node_index": 1,
                "start_group": "honest-network",
            },
            {
                "node_id": "adversarial-0",
                "node_type": "adversarial",
                "node_index": 0,
                "start_group": "adversarial-nodes",
            },
        ]
    )


def started_batch(node_id: str, node_type: str, node_index: int, event_id: str) -> dict:
    batch = event_batch()
    batch["events"][0]["event_id"] = event_id
    batch["node"] = {
        "node_id": node_id,
        "node_type": node_type,
        "node_index": node_index,
    }
    return batch


def barrier(client: TestClient, node_id: str):
    return client.get(
        "/api/v1/experiments/run-test/start-barrier", params={"node_id": node_id}
    )


def test_start_barrier_waits_for_every_node_in_the_group() -> None:
    client = TestClient(
        create_app(
            initial_registration=ExperimentRegistration.model_validate(two_node_registration())
        )
    )

    assert barrier(client, "normal-0").json() == {
        "experiment_id": "run-test",
        "node_id": "normal-0",
        "start_group": "all",
        "ready": False,
        "expected": 2,
        "started": 0,
        "started_nodes": [],
        "pending_nodes": ["normal-0", "normal-1"],
    }

    client.post("/api/v1/experiments/run-test/events", json=event_batch())

    response = barrier(client, "normal-0")
    assert response.json()["ready"] is False
    assert response.json()["started_nodes"] == ["normal-0"]
    assert response.json()["pending_nodes"] == ["normal-1"]

    client.post(
        "/api/v1/experiments/run-test/events",
        json=started_batch("normal-1", "normal", 1, "event-2"),
    )

    response = barrier(client, "normal-0")
    assert response.json()["ready"] is True
    assert response.json()["pending_nodes"] == []


def test_start_barrier_does_not_wait_for_a_later_startup_phase() -> None:
    client = TestClient(
        create_app(
            initial_registration=ExperimentRegistration.model_validate(staged_registration())
        )
    )

    for node_id, node_index, event_id in [("normal-0", 0, "event-1"), ("normal-1", 1, "event-2")]:
        client.post(
            "/api/v1/experiments/run-test/events",
            json=started_batch(node_id, "normal", node_index, event_id),
        )

    # The honest cohort is complete and must be released even though the
    # adversarial nodes have not started; that delay is the point of the phase.
    honest = barrier(client, "normal-0").json()
    assert honest["start_group"] == "honest-network"
    assert honest["ready"] is True
    assert honest["expected"] == 2

    late = barrier(client, "adversarial-0").json()
    assert late["start_group"] == "adversarial-nodes"
    assert late["ready"] is False
    assert late["pending_nodes"] == ["adversarial-0"]

    client.post(
        "/api/v1/experiments/run-test/events",
        json=started_batch("adversarial-0", "adversarial", 0, "event-3"),
    )
    assert barrier(client, "adversarial-0").json()["ready"] is True


def test_start_barrier_for_an_unknown_node_is_rejected() -> None:
    client = registered_app()

    response = barrier(client, "adversarial-0")

    assert response.status_code == 404
    assert response.json()["detail"] == "unknown node"


def test_start_barrier_ignores_events_that_are_not_node_started() -> None:
    client = TestClient(
        create_app(
            initial_registration=ExperimentRegistration.model_validate(two_node_registration())
        )
    )
    batch = event_batch()
    batch["events"][0]["event_type"] = "training_metrics"
    batch["events"][0]["data"] = {"step": 1, "round": 0, "loss": 1.0}

    client.post("/api/v1/experiments/run-test/events", json=batch)

    assert barrier(client, "normal-0").json()["started"] == 0


def test_start_barrier_for_unknown_experiment_is_rejected() -> None:
    client = registered_app()

    response = client.get(
        "/api/v1/experiments/run-other/start-barrier", params={"node_id": "normal-0"}
    )

    assert response.status_code == 404


def test_eval_metrics_event_is_accepted() -> None:
    client = registered_app()
    batch = event_batch()
    batch["events"][0]["event_type"] = "eval_metrics"
    batch["events"][0]["data"] = {
        "step": 4,
        "round": 1,
        "eval_loss": 0.4,
        "eval_accuracy": 0.9,
        "samples": 4096,
    }

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.status_code == 202


def exporting_app(export_path) -> TestClient:
    return TestClient(
        create_app(
            store=InMemoryEventStore(export_path=export_path),
            initial_registration=ExperimentRegistration.model_validate(registration()),
        )
    )


def test_health_reports_ok() -> None:
    assert registered_app().get("/health").json() == {"status": "ok"}


def test_event_id_reused_with_different_content_is_a_conflict() -> None:
    client = registered_app()
    client.post("/api/v1/experiments/run-test/events", json=event_batch())
    changed = event_batch()
    changed["events"][0]["data"] = {"runtime": "something-else"}

    response = client.post("/api/v1/experiments/run-test/events", json=changed)

    assert response.status_code == 409
    assert client.get("/api/v1/experiments/run-test/summary").json()["event_count"] == 1


def test_experiment_id_in_the_body_must_match_the_url() -> None:
    client = registered_app()
    batch = event_batch()
    batch["experiment_id"] = "run-other"

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.status_code == 400


def test_events_for_an_unknown_experiment_are_rejected() -> None:
    client = registered_app()
    batch = event_batch()
    batch["experiment_id"] = "run-other"

    response = client.post("/api/v1/experiments/run-other/events", json=batch)

    assert response.status_code == 404
    assert response.json()["detail"] == "unknown experiment"


def test_summary_for_an_unknown_experiment_is_rejected() -> None:
    assert registered_app().get("/api/v1/experiments/run-other/summary").status_code == 404


@pytest.mark.parametrize(("field", "value"), [("node_type", "adversarial"), ("node_index", 1)])
def test_node_must_match_the_manifest_in_every_field(field: str, value: object) -> None:
    """A known node_id is not enough: the type decides which side of the results
    an event lands on, so a node cannot report under another type."""
    client = registered_app()
    batch = event_batch()
    batch["node"][field] = value

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.status_code == 404
    assert response.json()["detail"] == "unknown node"


def test_unknown_field_on_an_event_is_rejected() -> None:
    client = registered_app()
    batch = event_batch()
    batch["events"][0]["round"] = 1

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.status_code == 422


def test_event_data_is_exported_without_the_aggregator_knowing_its_fields(tmp_path) -> None:
    """The data of an event belongs to the node type that sent it. A new node type
    can report a new field without the aggregator changing."""
    export_path = tmp_path / "events.jsonl"
    client = exporting_app(export_path)
    batch = event_batch()
    batch["events"][0]["data"] = {"runtime": "test", "a_field_from_a_new_node_type": {"nested": [1, 2]}}

    client.post("/api/v1/experiments/run-test/events", json=batch)

    exported = json.loads(export_path.read_text())
    assert exported["event"]["data"] == batch["events"][0]["data"]


def test_duplicates_and_rejected_batches_are_not_exported(tmp_path) -> None:
    export_path = tmp_path / "events.jsonl"
    client = exporting_app(export_path)
    unknown_node = event_batch()
    unknown_node["node"]["node_id"] = "adversarial-0"
    unknown_node["events"][0]["event_id"] = "event-2"

    client.post("/api/v1/experiments/run-test/events", json=event_batch())
    client.post("/api/v1/experiments/run-test/events", json=event_batch())
    client.post("/api/v1/experiments/run-test/events", json=unknown_node)

    assert len(export_path.read_text().splitlines()) == 1


def test_acknowledgement_reports_the_highest_sequence_in_the_batch() -> None:
    client = registered_app()
    batch = event_batch()
    second = dict(batch["events"][0], event_id="event-2", sequence=5)
    third = dict(batch["events"][0], event_id="event-3", sequence=3)
    batch["events"] += [second, third]

    response = client.post("/api/v1/experiments/run-test/events", json=batch)

    assert response.json() == {"accepted": 3, "duplicates": 0, "rejected": 0, "last_sequence": 5}


def test_an_experiment_cannot_be_registered_twice() -> None:
    store = InMemoryEventStore()
    store.register(ExperimentRegistration.model_validate(registration()))

    with pytest.raises(ExperimentAlreadyExists):
        store.register(ExperimentRegistration.model_validate(registration()))


def test_dht_snapshot_event_is_accepted() -> None:
    client = registered_app()
    batch = event_batch()
    batch["events"][0].update(event_type="dht_snapshot", data={
        "tick": 0, "tick_kind": "observation", "dht_id": "0" * 40, "known_peers": [],
    })
    response = client.post("/api/v1/experiments/run-test/events", json=batch)
    assert response.status_code == 202
    assert response.json()["accepted"] == 1
