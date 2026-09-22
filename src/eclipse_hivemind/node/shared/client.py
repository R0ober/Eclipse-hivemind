"""Small HTTP client for node events."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _send_event(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    sequence: int,
    event_type: str,
    data: dict,
    timeout: float = 5.0,
) -> dict:
    """Send one node event and return the aggregator acknowledgement."""
    payload = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "node": {
            "node_id": node_id,
            "node_type": node_type,
            "node_index": node_index,
        },
        "events": [
            {
                "event_id": str(uuid.uuid4()),
                "sequence": sequence,
                "event_type": event_type,
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "data": data,
            }
        ],
    }
    url = (
        f"{aggregator_endpoint.rstrip('/')}/api/v1/experiments/"
        f"{experiment_id}/events"
    )
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(
            f"aggregator rejected {event_type} with HTTP {error.code}: {body}"
        ) from error
    except URLError as error:
        raise RuntimeError(f"could not reach aggregator at {url}: {error.reason}") from error


def send_node_started(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    hivemind_address: str,
    timeout: float = 5.0,
) -> dict:
    """Send the first lifecycle event and return the aggregator acknowledgement."""
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        sequence=0,
        event_type="node_started",
        data={"runtime": "dht", "hivemind_address": hivemind_address},
        timeout=timeout,
    )


def send_training_metrics(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    step: int,
    round: int,
    loss: float,
    accuracy: float,
    samples: int,
    learning_rate: float,
    timeout: float = 5.0,
) -> dict:
    """Send one gradient-descent measurement from an honest node."""
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        sequence=step,
        event_type="training_metrics",
        data={
            "step": step,
            "round": round,
            "loss": loss,
            "accuracy": accuracy,
            "samples": samples,
            "learning_rate": learning_rate,
        },
        timeout=timeout,
    )
