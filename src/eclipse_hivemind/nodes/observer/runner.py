"""Join the DHT and report its neighbourhood without building a model."""

from __future__ import annotations

import os
import signal
import time

from shared import client
from shared.dht import resolved_dht_id, start_dht
from shared.topology import TICK_OBSERVATION, report_dht_snapshot


def observe_dht_on_timer(dht, aggregator_endpoint: str, identity: dict, settings: dict, ticks: int) -> None:
    """Report a fixed number of DHT observations at the configured interval.

    Take the first snapshot immediately, then wait snapshot_interval_seconds
    after each report before taking the next. Ticks start at zero and count
    observations independently of training or averaging rounds. Return as soon
    as the final report is sent, without another wait.

    Snapshot capture and delivery are shared with other node types; this loop
    defines the observer's schedule and passes through its target settings.
    """
    for tick in range(ticks):
        snapshot = report_dht_snapshot(
            dht,
            aggregator_endpoint=aggregator_endpoint,
            identity=identity,
            tick=tick,
            tick_kind=TICK_OBSERVATION,
            target_dht_id=settings["target_dht_id"],
            k_nearest=settings["k_nearest"],
        )
        print(f"DHT_SNAPSHOT_TICK={tick} KNOWN_PEERS={len(snapshot['known_peers'])}", flush=True)
        if tick + 1 < ticks:
            time.sleep(settings["snapshot_interval_seconds"])


def main() -> None:
    """Run the observer using the environment supplied by the orchestrator.

    Read the node identity and observation settings, join the DHT, and report
    the node's address and DHTID. Wait until every node in this startup group
    has reported its arrival before collecting snapshots, so the first view
    is taken after the group has joined.

    ROUNDS sets the number of observations for this node type. After the last
    report, emit the completion marker the harness expects. Always shut down
    the DHT when observation ends, a request fails, or a stop signal arrives.
    """
    endpoint = os.environ["AGGREGATOR_ENDPOINT"]
    identity = {"experiment_id": os.environ["EXPERIMENT_ID"], "node_id": os.environ["NODE_ID"],
                "node_type": os.environ["NODE_TYPE"], "node_index": int(os.environ["NODE_INDEX"])}
    settings = {
        "snapshot_interval_seconds": float(os.environ.get("PARAM_SNAPSHOT_INTERVAL_SECONDS", "1")),
        "target_dht_id": os.environ.get("PARAM_TARGET_DHT_ID") or None,
        "k_nearest": int(os.environ.get("PARAM_K_NEAREST", "20")),
        "start_barrier_timeout": float(os.environ.get("PARAM_START_BARRIER_TIMEOUT", "180")),
    }
    ticks = int(os.environ["ROUNDS"])
    if ticks <= 0 or settings["snapshot_interval_seconds"] <= 0 or settings["k_nearest"] <= 0:
        raise ValueError("ticks, snapshot interval and k_nearest must be greater than zero")

    def shutdown(_signum, _frame) -> None:
        """Exit through the cleanup block when the container is asked to stop."""
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    dht = start_dht()
    try:
        client.send_node_started(aggregator_endpoint=endpoint, **identity,
                                hivemind_address=str(dht.get_visible_maddrs()[0]),
                                settings=settings, dht_id=resolved_dht_id(dht))
        client.wait_for_start_barrier(aggregator_endpoint=endpoint,
                                     experiment_id=identity["experiment_id"],
                                     node_id=identity["node_id"], timeout=settings["start_barrier_timeout"])
        observe_dht_on_timer(dht, endpoint, identity, settings, ticks)
        # The harness uses this completion marker for every node task.
        print("TRAINING_COMPLETE=1", flush=True)
    finally:
        dht.shutdown()


if __name__ == "__main__":
    main()
