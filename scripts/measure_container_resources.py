"""Measure the resource usage of one Docker container under a fixed workload."""

from __future__ import annotations

import argparse
import json
import shlex
import time
import uuid
from pathlib import Path

import docker
from docker.errors import APIError, ImageNotFound


def _memory_usage(stats: dict) -> int:
    """Return cgroup memory usage with Docker's cache excluded when available."""
    memory = stats.get("memory_stats", {})
    usage = int(memory.get("usage", 0))
    cache = memory.get("stats", {}).get("inactive_file", 0)
    if not cache:
        cache = memory.get("stats", {}).get("total_inactive_file", 0)
    return max(0, usage - int(cache))


def _cpu_percent(stats: dict, previous: dict | None) -> float | None:
    if previous is None:
        return None

    current_cpu = stats.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage")
    previous_cpu = previous.get("cpu_stats", {}).get("cpu_usage", {}).get("total_usage")
    current_system = stats.get("cpu_stats", {}).get("system_cpu_usage")
    previous_system = previous.get("cpu_stats", {}).get("system_cpu_usage")
    if None in (current_cpu, previous_cpu, current_system, previous_system):
        return None

    cpu_delta = current_cpu - previous_cpu
    system_delta = current_system - previous_system
    online_cpus = stats.get("cpu_stats", {}).get("online_cpus") or 1
    if cpu_delta <= 0 or system_delta <= 0:
        return 0.0
    return 100.0 * cpu_delta / system_delta * online_cpus


def _wait_for_log(container, pattern: str, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        output = container.logs(tail=100)
        text = output.decode(errors="replace") if isinstance(output, bytes) else str(output)
        if pattern in text:
            return
        container.reload()
        if container.status == "exited":
            raise RuntimeError(f"container exited before readiness marker: {text[-500:]}")
        time.sleep(0.25)
    raise TimeoutError(f"timed out waiting for readiness marker {pattern!r}")


def measure_container(
    client,
    *,
    image: str,
    command: list[str] | None,
    network: str,
    duration: float,
    interval: float,
    ready_pattern: str | None,
    ready_timeout: float,
) -> dict:
    container_name = f"eclipse-resource-measure-{uuid.uuid4().hex[:10]}"
    container = None
    samples: list[dict] = []
    started_at = time.monotonic()

    try:
        image_info = client.images.get(image)
        container = client.containers.run(
            image,
            command=command,
            name=container_name,
            network=network,
            detach=True,
            labels={"eclipse_resource_measurement": "true"},
        )
        if ready_pattern:
            _wait_for_log(container, ready_pattern, ready_timeout)

        ready_at = time.monotonic()
        previous_stats = None
        deadline = ready_at + duration
        while time.monotonic() < deadline:
            stats = container.stats(stream=False)
            memory_bytes = _memory_usage(stats)
            cpu_percent = _cpu_percent(stats, previous_stats)
            samples.append(
                {
                    "elapsed_seconds": round(time.monotonic() - ready_at, 3),
                    "memory_bytes": memory_bytes,
                    "cpu_percent": cpu_percent,
                    "pids": stats.get("pids_stats", {}).get("current"),
                }
            )
            previous_stats = stats
            time.sleep(interval)

        memory_values = [sample["memory_bytes"] for sample in samples]
        cpu_values = [sample["cpu_percent"] for sample in samples if sample["cpu_percent"] is not None]
        return {
            "image": image,
            "image_size_bytes": image_info.attrs.get("Size"),
            "command": command,
            "network": network,
            "duration_seconds": duration,
            "interval_seconds": interval,
            "startup_seconds": round(ready_at - started_at, 3),
            "sample_count": len(samples),
            "memory": {
                "peak_bytes": max(memory_values, default=0),
                "average_bytes": round(sum(memory_values) / len(memory_values)) if memory_values else 0,
            },
            "cpu_percent": {
                "peak": round(max(cpu_values), 2) if cpu_values else None,
                "average": round(sum(cpu_values) / len(cpu_values), 2) if cpu_values else None,
            },
            "peak_pids": max((sample["pids"] or 0 for sample in samples), default=0),
            "samples": samples,
        }
    finally:
        if container is not None:
            container.remove(force=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True, help="Docker image to measure")
    parser.add_argument(
        "--command",
        help="Optional command string; omit it to use the image CMD",
    )
    parser.add_argument("--network", default="bridge")
    parser.add_argument("--duration", type=float, default=30.0)
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument(
        "--ready-pattern",
        help="Optional log text to wait for before sampling",
    )
    parser.add_argument("--ready-timeout", type=float, default=120.0)
    parser.add_argument("--output", type=Path, help="Write JSON to this path")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.duration <= 0 or args.interval <= 0 or args.ready_timeout <= 0:
        raise SystemExit("duration, interval, and ready-timeout must be positive")

    command = shlex.split(args.command) if args.command else None
    try:
        result = measure_container(
            docker.from_env(),
            image=args.image,
            command=command,
            network=args.network,
            duration=args.duration,
            interval=args.interval,
            ready_pattern=args.ready_pattern,
            ready_timeout=args.ready_timeout,
        )
    except ImageNotFound as error:
        raise SystemExit(f"Docker image not found: {error}") from error
    except APIError as error:
        raise SystemExit(f"Docker API error: {error}") from error

    text = json.dumps(result, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
