"""Honest node: joins the DHT, trains, and reports experiment events."""

from __future__ import annotations

import hashlib
import os
import signal
import time
from datetime import datetime, timezone

import hivemind
import torch

from shared import averaging_watch, client
from shared.dht import start_dht


def generate_batch(size: int, generator: torch.Generator) -> tuple[torch.Tensor, torch.Tensor]:
    features = torch.randn(size, 2, generator=generator)
    return features, (features[:, 0] * features[:, 1] > 0).long()


def train_and_report(
    dht: hivemind.DHT,
    aggregator_endpoint: str,
    identity: dict,
    settings: dict,
    model: torch.nn.Module,
    experiment_seed: int,
    node_seed: int,
    rounds: int,
) -> None:
    watcher = averaging_watch.attach()
    optimizer = hivemind.Optimizer(
        dht=dht,
        run_id=f"{identity['experiment_id']}:toy-classifier:v1",
        target_batch_size=settings["target_batch_size"],
        batch_size_per_step=settings["batch_size"],
        optimizer=torch.optim.SGD(model.parameters(), lr=settings["learning_rate"], momentum=0.9),
        matchmaking_time=settings["matchmaking_time"],
        averaging_timeout=settings["averaging_timeout"],
        use_local_updates=False,
        verbose=True,
    )
    eval_features, eval_labels = generate_batch(
        settings["eval_size"], torch.Generator().manual_seed(experiment_seed)
    )
    data_generator = torch.Generator().manual_seed(node_seed)

    try:
        step = 0
        epoch = optimizer.local_epoch
        while optimizer.local_epoch < rounds:
            step += 1
            features, labels = generate_batch(settings["batch_size"], data_generator)
            logits = model(features)
            loss = torch.nn.functional.cross_entropy(logits, labels)
            optimizer.zero_grad()
            loss.backward()
            watcher.take()
            optimizer.step(batch_size=settings["batch_size"])

            batch_accuracy = (logits.argmax(dim=1) == labels).float().mean().item()
            client.send_training_metrics(
                aggregator_endpoint=aggregator_endpoint,
                **identity,
                step=step,
                round=optimizer.local_epoch,
                loss=loss.item(),
                batch_accuracy=batch_accuracy,
                samples=settings["batch_size"],
                learning_rate=settings["learning_rate"],
            )
            print(
                f"TRAINING_STEP={step} ROUND={optimizer.local_epoch} "
                f"LOSS={loss.item():.6f} BATCH_ACCURACY={batch_accuracy:.4f}",
                flush=True,
            )

            if optimizer.local_epoch != epoch:
                observation = averaging_watch.interpret(watcher.take())
                if observation["started_at"] is not None:
                    client.send_averaging_started(
                        aggregator_endpoint=aggregator_endpoint,
                        **identity,
                        step=step,
                        round=epoch,
                        occurred_at=datetime.fromtimestamp(observation["started_at"], timezone.utc),
                    )
                client.send_averaging_completed(
                    aggregator_endpoint=aggregator_endpoint,
                    **identity,
                    step=step,
                    round=epoch,
                    observed=observation["observed"],
                    success=observation["success"],
                    fallback=observation["fallback"],
                    fallback_reason=observation["fallback_reason"],
                    group_size=observation["group_size"],
                    duration_seconds=observation["duration_seconds"],
                )
                with torch.no_grad():
                    eval_logits = model(eval_features)
                    eval_loss = torch.nn.functional.cross_entropy(eval_logits, eval_labels).item()
                    eval_accuracy = (eval_logits.argmax(dim=1) == eval_labels).float().mean().item()
                client.send_eval_metrics(
                    aggregator_endpoint=aggregator_endpoint,
                    **identity,
                    step=step,
                    round=optimizer.local_epoch,
                    eval_loss=eval_loss,
                    eval_accuracy=eval_accuracy,
                    samples=settings["eval_size"],
                )
                epoch = optimizer.local_epoch

            time.sleep(settings["step_delay_seconds"])
    finally:
        optimizer.shutdown()


def main() -> None:
    aggregator_endpoint = os.environ["AGGREGATOR_ENDPOINT"]
    identity = {
        "experiment_id": os.environ["EXPERIMENT_ID"],
        "node_id": os.environ["NODE_ID"],
        "node_type": os.environ["NODE_TYPE"],
        "node_index": int(os.environ["NODE_INDEX"]),
    }
    batch_size = int(os.environ.get("PARAM_BATCH_SIZE", "32"))
    peer_count = int(os.environ.get("EXPERIMENT_TOTAL_NODES", "1"))
    settings = {
        "batch_size": batch_size,
        "learning_rate": float(os.environ.get("PARAM_LEARNING_RATE", "0.05")),
        "target_batch_size": int(os.environ.get("PARAM_TARGET_BATCH_SIZE", str(peer_count * batch_size * 2))),
        "matchmaking_time": float(os.environ.get("PARAM_MATCHMAKING_TIME", "15")),
        "averaging_timeout": float(os.environ.get("PARAM_AVERAGING_TIMEOUT", "60")),
        "step_delay_seconds": float(os.environ.get("PARAM_STEP_DELAY_SECONDS", "1")),
        "eval_size": int(os.environ.get("PARAM_EVAL_SIZE", "4096")),
        "start_barrier_timeout": float(os.environ.get("PARAM_START_BARRIER_TIMEOUT", "180")),
    }
    if min(settings["batch_size"], settings["target_batch_size"], settings["eval_size"]) <= 0:
        raise ValueError("batch sizes must be greater than zero")
    if settings["matchmaking_time"] <= 0 or settings["averaging_timeout"] <= settings["matchmaking_time"]:
        raise ValueError("averaging timeout must be greater than matchmaking time")
    if settings["step_delay_seconds"] < 0:
        raise ValueError("step delay must not be negative")

    rounds = int(os.environ["ROUNDS"])
    if rounds <= 0:
        raise ValueError("ROUNDS must be greater than zero")
    experiment_seed = int(os.environ["EXPERIMENT_SEED"])
    node_seed = int(os.environ["NODE_SEED"])

    torch.set_num_threads(1)
    torch.manual_seed(experiment_seed)
    model = torch.nn.Sequential(torch.nn.Linear(2, 8), torch.nn.ReLU(), torch.nn.Linear(8, 2))
    digest = hashlib.sha256()
    for name, tensor in sorted(model.state_dict().items()):
        digest.update(name.encode())
        digest.update(tensor.detach().cpu().numpy().tobytes())

    dht = start_dht()

    def shutdown(_signum: int, _frame: object) -> None:
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    try:
        acknowledgement = client.send_node_started(
            aggregator_endpoint=aggregator_endpoint,
            **identity,
            hivemind_address=str(dht.get_visible_maddrs()[0]),
            settings=settings,
            model_fingerprint=digest.hexdigest()[:16],
        )
        print(f"AGGREGATOR_ACK={acknowledgement}", flush=True)
        try:
            barrier = client.wait_for_start_barrier(
                aggregator_endpoint=aggregator_endpoint,
                experiment_id=identity["experiment_id"],
                node_id=identity["node_id"],
                timeout=settings["start_barrier_timeout"],
            )
            print(f"START_BARRIER_READY group={barrier['start_group']} nodes={barrier['started']}", flush=True)
        except client.StartBarrierTimeout as error:
            print(f"START_BARRIER_TIMEOUT {error}", flush=True)
            client.send_node_error(
                aggregator_endpoint=aggregator_endpoint,
                **identity,
                error_code="start_barrier_timeout",
                message=str(error),
                recoverable=True,
            )
        train_and_report(dht, aggregator_endpoint, identity, settings, model, experiment_seed, node_seed, rounds)
        print("TRAINING_COMPLETE=1", flush=True)
    finally:
        dht.shutdown()


if __name__ == "__main__":
    main()
