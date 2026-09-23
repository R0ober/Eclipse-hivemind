"""Small HTTP client for node events."""

from __future__ import annotations

import itertools
import json
import time
import uuid
from datetime import datetime, timezone
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# One node-local sequence per process, so every event type shares the same counter
# and the aggregator can spot a gap. node_started therefore always gets sequence 0.
_sequence = itertools.count()


class StartBarrierTimeout(RuntimeError):
    """Raised when not every node reported node_started before the timeout."""

    def __init__(self, barrier: dict) -> None:
        super().__init__(
            f"start barrier for group {barrier['start_group']!r} timed out with "
            f"{barrier['started']}/{barrier['expected']} nodes started, "
            f"still waiting for {barrier['pending_nodes']}"
        )
        self.barrier = barrier


def _send_event(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    event_type: str,
    data: dict,
    occurred_at: datetime | None = None,
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
                "sequence": next(_sequence),
                "event_type": event_type,
                "occurred_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
                "data": data,
            }
        ],
    }
    url = f"{aggregator_endpoint.rstrip('/')}/api/v1/experiments/{experiment_id}/events"
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


def _get_json(url: str, timeout: float) -> dict:
    try:
        with urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode("utf-8"))
    except HTTPError as error:
        body = error.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"aggregator returned HTTP {error.code} for {url}: {body}") from error
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
    settings: dict | None = None,
    model_fingerprint: str | None = None,
    timeout: float = 5.0,
) -> dict:
    """Send the first lifecycle event and return the aggregator acknowledgement.

    `settings` and `model_fingerprint` are reported so a run can be checked
    afterwards for peers that disagree on the averaging settings or that did not
    start from the same weights.
    """
    data = {"runtime": "dht", "hivemind_address": hivemind_address}
    if settings is not None:
        data["settings"] = settings
    if model_fingerprint is not None:
        data["model_fingerprint"] = model_fingerprint
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="node_started",
        data=data,
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
    batch_accuracy: float,
    samples: int,
    learning_rate: float,
    timeout: float = 5.0,
) -> dict:
    """Send one gradient-descent measurement from an honest node.

    `batch_accuracy` is measured on the local training batch, so it is noisy by
    construction. Held-out accuracy is reported by `send_eval_metrics`.
    """
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="training_metrics",
        data={
            "step": step,
            "round": round,
            "loss": loss,
            "batch_accuracy": batch_accuracy,
            "samples": samples,
            "learning_rate": learning_rate,
        },
        timeout=timeout,
    )


def send_eval_metrics(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    step: int,
    round: int,
    eval_loss: float,
    eval_accuracy: float,
    samples: int,
    timeout: float = 5.0,
) -> dict:
    """Send one measurement on the held-out evaluation set shared by every node."""
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="eval_metrics",
        data={
            "step": step,
            "round": round,
            "eval_loss": eval_loss,
            "eval_accuracy": eval_accuracy,
            "samples": samples,
        },
        timeout=timeout,
    )


def send_averaging_started(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    step: int,
    round: int,
    occurred_at: datetime | None = None,
    timeout: float = 5.0,
) -> dict:
    """Report that hivemind began an aggregation round for this epoch."""
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="averaging_started",
        data={"step": step, "round": round, "local_epoch": round},
        occurred_at=occurred_at,
        timeout=timeout,
    )


def send_averaging_completed(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    step: int,
    round: int,
    observed: bool,
    success: bool,
    fallback: bool,
    fallback_reason: str | None,
    group_size: int | None,
    duration_seconds: float | None,
    occurred_at: datetime | None = None,
    timeout: float = 5.0,
) -> dict:
    """Report how an aggregation round ended: averaged, or fell back to local gradients.

    `observed` is False when the epoch advanced without an averaging round of its
    own, which happens when hivemind reloads state from a peer that is ahead.
    """
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="averaging_completed",
        data={
            "step": step,
            "round": round,
            "local_epoch": round,
            "observed": observed,
            "success": success,
            "fallback": fallback,
            "fallback_reason": fallback_reason,
            "group_size": group_size,
            "duration_seconds": duration_seconds,
        },
        occurred_at=occurred_at,
        timeout=timeout,
    )


def send_node_error(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    error_code: str,
    message: str,
    recoverable: bool,
    round: int | None = None,
    timeout: float = 5.0,
) -> dict:
    """Report a problem the node handled locally, so it stays visible in the export."""
    return _send_event(
        aggregator_endpoint=aggregator_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="node_error",
        data={
            "error_code": error_code,
            "message": message,
            "recoverable": recoverable,
            "round": round,
        },
        timeout=timeout,
    )


def wait_for_start_barrier(
    *,
    aggregator_endpoint: str,
    experiment_id: str,
    node_id: str,
    timeout: float,
    poll_interval: float = 0.2,
    request_timeout: float = 5.0,
) -> dict:
    """Block until every node in this node's start group has reported node_started.

    Returns the barrier state once it is ready, and raises StartBarrierTimeout
    with the last observed state if it is not ready in time. Without this, a node
    that starts first trains alone against an empty swarm. The barrier covers this
    node's cohort only, so a later startup phase joins a swarm already in progress.
    """
    url = (
        f"{aggregator_endpoint.rstrip('/')}/api/v1/experiments/"
        f"{experiment_id}/start-barrier?node_id={quote(node_id)}"
    )
    deadline = time.monotonic() + timeout
    while True:
        barrier = _get_json(url, timeout=request_timeout)
        if barrier["ready"]:
            return barrier
        if time.monotonic() >= deadline:
            raise StartBarrierTimeout(barrier)
        time.sleep(poll_interval)
