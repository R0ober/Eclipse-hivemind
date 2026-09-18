"""Small HTTP client for node events."""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


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
                "sequence": 0,
                "event_type": "node_started",
                "occurred_at": datetime.now(timezone.utc).isoformat(),
                "data": {
                    "runtime": "dht",
                    "hivemind_address": hivemind_address,
                },
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
            f"aggregator rejected node_started with HTTP {error.code}: {body}"
        ) from error
    except URLError as error:
        raise RuntimeError(f"could not reach aggregator at {url}: {error.reason}") from error