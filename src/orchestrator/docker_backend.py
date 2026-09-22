from __future__ import annotations

import time

import docker

from .backend import ContainerBackend


class DockerBackend(ContainerBackend):
    """Docker SDK implementation of the orchestrator backend."""

    def __init__(self) -> None:
        self._client = docker.from_env()

    def create_network(self, name: str, labels: dict[str, str]) -> str:
        network = self._client.networks.create(name,driver="bridge", labels=labels)
        return network.id

    def create_container(
        self,
        *,
        image: str,
        name: str,
        network: str,
        env: dict[str, str],
        labels: dict[str, str],
        volumes: dict | None = None,
        command: list[str] | str | None = None,
        network_aliases: list[str] | None = None,
    ) -> str:
        options = {
            "image": image,
            "name": name,
            "environment": env,
            "labels": labels,
            "detach": True,
        }
        if network_aliases is None:
            options["network"] = network
        if volumes is not None:
            options["volumes"] = volumes
        if command is not None:
            options["command"] = command
        container = self._client.containers.create(**options)
        if network_aliases is not None:
            self._client.networks.get(network).connect(
                container,
                aliases=network_aliases,
            )
        return container.id

    def start(self, container_id: str) -> None:
        self._client.containers.get(container_id).start()

    def wait_for_log(self, container_id: str, pattern: str, timeout: float) -> str:
        """Return the first log line containing ``pattern`` before timeout."""
        container = self._client.containers.get(container_id)
        deadline = time.monotonic() + timeout

        while time.monotonic() < deadline:
            logs = container.logs(tail=100)
            text = logs.decode(errors="replace") if isinstance(logs, bytes) else str(logs)
            for line in text.splitlines():
                if pattern in line:
                    return line

            container.reload()
            if container.status == "exited":
                raise RuntimeError(f"container {container_id} exited: {text}")

            time.sleep(min(1.0, max(0.0, deadline - time.monotonic())))

        raise TimeoutError(
            f"Timed out waiting for '{pattern}' in container {container_id}"
        )

    def stop(self, container_id: str) -> None:
        self._client.containers.get(container_id).stop()

    def remove_container(self, container_id: str) -> None:
        self._client.containers.get(container_id).remove(force=True)

    def remove_network(self, name: str) -> None:
        self._client.networks.get(name).remove()
