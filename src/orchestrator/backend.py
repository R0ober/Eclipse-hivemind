"""Backend contract used by the orchestrator and its tests."""

from __future__ import annotations

from typing import Protocol, runtime_checkable


@runtime_checkable
class ContainerBackend(Protocol):
    def create_network(self, name: str, labels: dict[str, str]) -> str: ...

    def remove_network(self, name: str) -> None: ...

    def create_container(
        self,
        *,
        image: str,
        name: str,
        network: str,
        env: dict[str, str],
        labels: dict[str, str],
        volumes: dict | None = None,
    ) -> str: ...

    def start(self, container_id: str) -> None: ...

    def wait_for_log(self, container_id: str, pattern: str, timeout: float) -> str: ...

    def stop(self, container_id: str) -> None: ...

    def remove_container(self, container_id: str) -> None: ...
