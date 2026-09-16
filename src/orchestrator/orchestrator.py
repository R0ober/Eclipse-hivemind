"""Create and clean up the Docker network for a validated experiment."""

from __future__ import annotations

import contextlib
import datetime as _dt
import secrets
from collections.abc import Iterator

from eclipse_hivemind.config import ExperimentConfig
from .backend import ContainerBackend


def _new_run_id() -> str:
    timestamp = _dt.datetime.now(_dt.timezone.utc).strftime("%Y%m%d-%H%M%S")
    return f"run-{timestamp}-{secrets.token_hex(3)}"


@contextlib.contextmanager
def run(
    config: ExperimentConfig,
    backend: ContainerBackend,
    run_id: str | None = None,
) -> Iterator[str]:
    """Create the run network, yield its ID, and remove it on exit."""
    del config
    run_id = run_id or _new_run_id()
    network_name = f"eclipse-{run_id}"
    network_id = backend.create_network(network_name, {"eclipse_run": run_id})
    try:
        yield network_id
    finally:
        with contextlib.suppress(Exception):
            backend.remove_network(network_id)
