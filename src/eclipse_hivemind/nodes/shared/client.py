"""Small HTTP client for node events."""

from __future__ import annotations

import itertools
import json
import os
import time
import uuid
from datetime import datetime, timezone
from functools import wraps
from urllib.error import HTTPError, URLError
from urllib.parse import quote
from urllib.request import Request, urlopen

# One node-local sequence per process, so every event type shares the same counter
# and the logger can spot a gap. node_started therefore always gets sequence 0.
_sequence = itertools.count()
LOGGER_REQUEST_TIMEOUT_SECONDS = 30.0
EVENT_POST_ATTEMPTS = 3
IDEMPOTENT_GET_ATTEMPTS = 3


class StartBarrierTimeout(RuntimeError):
    """Raised when not every node reported node_started before the timeout."""

    def __init__(self, barrier: dict) -> None:
        super().__init__(
            f"start barrier for group {barrier['start_group']!r} timed out with "
            f"{barrier['started']}/{barrier['expected']} nodes started, "
            f"still waiting for {barrier['pending_nodes']}"
        )
        self.barrier = barrier


def logger_endpoint_from_env() -> str:
    """Read the Logger endpoint, accepting environments from existing images."""
    return os.environ.get("LOGGER_ENDPOINT") or os.environ["AGGREGATOR_ENDPOINT"]


def _accept_legacy_endpoint(function):
    """Keep inherited training runners in versioned node images callable."""
    @wraps(function)
    def compatible(*args, **kwargs):
        if "aggregator_endpoint" in kwargs:
            if "logger_endpoint" in kwargs:
                raise TypeError("specify only logger_endpoint")
            kwargs["logger_endpoint"] = kwargs.pop("aggregator_endpoint")
        return function(*args, **kwargs)
    return compatible


@_accept_legacy_endpoint
def _send_event(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    event_type: str,
    data: dict,
    occurred_at: datetime | None = None,
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Send one node event and return the logger acknowledgement."""
    event = {
        "event_id": str(uuid.uuid4()),
        "sequence": next(_sequence),
        "event_type": event_type,
        "occurred_at": (occurred_at or datetime.now(timezone.utc)).isoformat(),
        "data": data,
    }
    return _post_events(
        logger_endpoint=logger_endpoint, experiment_id=experiment_id,
        node_id=node_id, node_type=node_type, node_index=node_index,
        events=[event], timeout=timeout, event_label=event_type,
    )


@_accept_legacy_endpoint
def send_eclipse_observations(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    observations: list[dict],
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Send ordered observations in one request while retaining per-event IDs/times."""
    events = []
    for data in observations:
        event_time = datetime.fromtimestamp(data["dht_time"], timezone.utc)
        events.append({
            "event_id": str(uuid.uuid4()),
            "sequence": next(_sequence),
            "event_type": "eclipse_observation",
            "occurred_at": event_time.isoformat(),
            "data": data,
        })
    return _post_events(
        logger_endpoint=logger_endpoint, experiment_id=experiment_id,
        node_id=node_id, node_type=node_type, node_index=node_index,
        events=events, timeout=timeout, event_label="eclipse_observation batch",
    )


@_accept_legacy_endpoint
def _post_events(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    events: list[dict],
    timeout: float,
    event_label: str,
) -> dict:
    payload = {
        "schema_version": 1,
        "experiment_id": experiment_id,
        "node": {"node_id": node_id, "node_type": node_type, "node_index": node_index},
        "events": events,
    }
    url = f"{logger_endpoint.rstrip('/')}/api/v1/experiments/{experiment_id}/events"
    request = Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    for attempt in range(EVENT_POST_ATTEMPTS):
        try:
            with urlopen(request, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"logger rejected {event_label} with HTTP {error.code}: {body}") from error
        except (URLError, TimeoutError) as error:
            reason = error.reason if isinstance(error, URLError) else error
            if attempt + 1 == EVENT_POST_ATTEMPTS:
                raise RuntimeError(f"could not deliver {event_label} to {url} after {EVENT_POST_ATTEMPTS} attempts: {reason}") from error
            time.sleep(0.25 * (2 ** attempt))


def _get_json(url: str, timeout: float) -> dict:
    for attempt in range(IDEMPOTENT_GET_ATTEMPTS):
        try:
            with urlopen(url, timeout=timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except HTTPError as error:
            body = error.read().decode("utf-8", errors="replace")
            raise RuntimeError(f"logger returned HTTP {error.code} for {url}: {body}") from error
        except (URLError, TimeoutError) as error:
            reason = error.reason if isinstance(error, URLError) else error
            if attempt + 1 == IDEMPOTENT_GET_ATTEMPTS:
                raise RuntimeError(f"could not reach logger at {url} after {IDEMPOTENT_GET_ATTEMPTS} attempts: {reason}") from error
            time.sleep(0.25 * (2 ** attempt))


@_accept_legacy_endpoint
def send_node_started(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    hivemind_address: str,
    settings: dict | None = None,
    model_fingerprint: str | None = None,
    runtime_metadata: dict | None = None,
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Send the first lifecycle event and return the logger acknowledgement.

    `settings` and `model_fingerprint` are reported so a run can be checked
    afterwards for peers that disagree on the averaging settings or that did not
    start from the same weights.
    """
    data = {"runtime": "dht", "hivemind_address": hivemind_address}
    if settings is not None:
        data["settings"] = settings
    if model_fingerprint is not None:
        data["model_fingerprint"] = model_fingerprint
    if runtime_metadata is not None:
        data["runtime_metadata"] = runtime_metadata
    return _send_event(
        logger_endpoint=logger_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="node_started",
        data=data,
        timeout=timeout,
    )


@_accept_legacy_endpoint
def send_training_metrics(
    *,
    logger_endpoint: str,
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
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Send one gradient-descent measurement from an honest node.

    `batch_accuracy` is measured on the local training batch, so it is noisy by
    construction. Held-out accuracy is reported by `send_eval_metrics`.
    """
    return _send_event(
        logger_endpoint=logger_endpoint,
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


@_accept_legacy_endpoint
def send_eval_metrics(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    step: int,
    round: int,
    eval_loss: float,
    eval_accuracy: float,
    predicted_class_fractions: list[float],
    samples: int,
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Send one measurement on the held-out evaluation set shared by every node.

    `predicted_class_fractions` carries how the predictions were spread across the
    classes. Accuracy on its own cannot separate a model that is learning from one
    that has collapsed onto a single class, and both a healthy run and an attacked
    run can sit at the same accuracy while predicting very different things.
    """
    return _send_event(
        logger_endpoint=logger_endpoint,
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
            "predicted_class_fractions": predicted_class_fractions,
            "samples": samples,
        },
        timeout=timeout,
    )


@_accept_legacy_endpoint
def send_averaging_started(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    step: int,
    round: int,
    occurred_at: datetime | None = None,
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Report that hivemind began an aggregation round for this epoch."""
    return _send_event(
        logger_endpoint=logger_endpoint,
        experiment_id=experiment_id,
        node_id=node_id,
        node_type=node_type,
        node_index=node_index,
        event_type="averaging_started",
        data={"step": step, "round": round, "local_epoch": round},
        occurred_at=occurred_at,
        timeout=timeout,
    )


@_accept_legacy_endpoint
def send_averaging_completed(
    *,
    logger_endpoint: str,
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
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Report how an aggregation round ended: averaged, or fell back to local gradients.

    `observed` is False when the epoch advanced without an averaging round of its
    own, which happens when hivemind reloads state from a peer that is ahead.
    """
    return _send_event(
        logger_endpoint=logger_endpoint,
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


@_accept_legacy_endpoint
def send_node_error(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    node_type: str,
    node_index: int,
    error_code: str,
    message: str,
    recoverable: bool,
    round: int | None = None,
    timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Report a problem the node handled locally, so it stays visible in the export."""
    return _send_event(
        logger_endpoint=logger_endpoint,
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


@_accept_legacy_endpoint
def wait_for_start_barrier(
    *,
    logger_endpoint: str,
    experiment_id: str,
    node_id: str,
    timeout: float,
    poll_interval: float = 0.2,
    request_timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS,
) -> dict:
    """Block until every node in this node's start group has reported node_started.

    Returns the barrier state once it is ready, and raises StartBarrierTimeout
    with the last observed state if it is not ready in time. Without this, a node
    that starts first trains alone against an empty swarm. The barrier covers this
    node's cohort only, so a later startup phase joins a swarm already in progress.
    """
    url = (
        f"{logger_endpoint.rstrip('/')}/api/v1/experiments/"
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


def send_eclipse_observation(*, data: dict, **kwargs) -> dict:
    return _send_event(event_type="eclipse_observation", data=data, **kwargs)


def send_node_finished(**kwargs) -> dict:
    return _send_event(event_type="node_finished", data={}, **kwargs)


@_accept_legacy_endpoint
def get_runtime(*, logger_endpoint: str, experiment_id: str,
                timeout: float = LOGGER_REQUEST_TIMEOUT_SECONDS) -> dict:
    return _get_json(f"{logger_endpoint.rstrip('/')}/api/v1/experiments/{experiment_id}/runtime", timeout)
