"""Honest node: joins the DHT and reports lifecycle events to the aggregator."""

from __future__ import annotations

import os
import signal

import hivemind
import torch

from shared import client
from shared.dht import start_dht


def _positive_int(name: str, default: int) -> int:
    value = int(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str, default: float) -> float:
    value = float(os.environ.get(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def train_and_report(dht: hivemind.DHT, aggregator_endpoint: str) -> None:
    """Train a compact nonlinear classifier with Hivemind aggregation."""
    rounds = _positive_int("ROUNDS", 1)
    batch_size = _positive_int("PARAM_BATCH_SIZE", 32)
    learning_rate = _positive_float("PARAM_LEARNING_RATE", 0.05)
    target_batch_size = _positive_int("PARAM_TARGET_BATCH_SIZE", batch_size)
    matchmaking_time = _positive_float("PARAM_MATCHMAKING_TIME", 5.0)
    torch.set_num_threads(1)
    torch.manual_seed(int(os.environ.get("EXPERIMENT_SEED", "0")))
    data_generator = torch.Generator().manual_seed(
        int(os.environ.get("NODE_SEED", "0"))
    )

    model = torch.nn.Sequential(
        torch.nn.Linear(2, 8),
        torch.nn.ReLU(),
        torch.nn.Linear(8, 2),
    )
    optimizer = hivemind.Optimizer(
        dht=dht,
        run_id=f"{os.environ.get('EXPERIMENT_ID', 'unknown')}:toy-classifier:v1",
        target_batch_size=target_batch_size,
        batch_size_per_step=batch_size,
        optimizer=torch.optim.SGD(
            model.parameters(),
            lr=learning_rate,
            momentum=0.9,
        ),
        matchmaking_time=matchmaking_time,
        averaging_timeout=10.0,
        use_local_updates=False,
    )

    try:
        step = 0
        while optimizer.local_epoch < rounds:
            step += 1
            features = torch.randn(batch_size, 2, generator=data_generator)
            labels = (features[:, 0] * features[:, 1] > 0).long()
            logits = model(features)
            loss = torch.nn.functional.cross_entropy(logits, labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step(batch_size=batch_size)

            accuracy = (logits.argmax(dim=1) == labels).float().mean().item()
            acknowledgement = client.send_training_metrics(
                aggregator_endpoint=aggregator_endpoint,
                experiment_id=os.environ.get("EXPERIMENT_ID", "unknown"),
                node_id=os.environ.get("NODE_ID", "unknown"),
                node_type=os.environ.get("NODE_TYPE", "unknown"),
                node_index=int(os.environ.get("NODE_INDEX", "0")),
                step=step,
                round=optimizer.local_epoch,
                loss=loss.item(),
                accuracy=accuracy,
                samples=batch_size,
                learning_rate=learning_rate,
            )
            print(
                f"TRAINING_STEP={step} LOSS={loss.item():.6f} "
                f"ACCURACY={accuracy:.4f} "
                f"AGGREGATOR_ACK={acknowledgement}",
                flush=True,
            )
    finally:
        optimizer.shutdown()


def main() -> None:
    dht = start_dht()

    def shutdown(_signum: int, _frame: object) -> None:
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    aggregator_endpoint = os.environ.get("AGGREGATOR_ENDPOINT")
    if not aggregator_endpoint:
        raise RuntimeError("AGGREGATOR_ENDPOINT is required")

    try:
        visible_maddrs = dht.get_visible_maddrs()
        acknowledgement = client.send_node_started(
            aggregator_endpoint=aggregator_endpoint,
            experiment_id=os.environ.get("EXPERIMENT_ID", "unknown"),
            node_id=os.environ.get("NODE_ID", "unknown"),
            node_type=os.environ.get("NODE_TYPE", "unknown"),
            node_index=int(os.environ.get("NODE_INDEX", "0")),
            hivemind_address=str(visible_maddrs[0]),
        )
        print(f"AGGREGATOR_ACK={acknowledgement}", flush=True)
        train_and_report(dht, aggregator_endpoint)
        print("TRAINING_COMPLETE=1", flush=True)
    finally:
        dht.shutdown()


if __name__ == "__main__":
    main()
