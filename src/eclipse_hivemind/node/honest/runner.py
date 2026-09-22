"""Honest node: joins the DHT and reports lifecycle events to the aggregator."""

from __future__ import annotations

import os
import signal
import time

from shared import client
from shared.dht import start_dht


def main() -> None:
    dht = start_dht()

    def shutdown(_signum: int, _frame: object) -> None:
        dht.shutdown()
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    aggregator_endpoint = os.environ.get("AGGREGATOR_ENDPOINT")
    if not aggregator_endpoint:
        raise RuntimeError("AGGREGATOR_ENDPOINT is required")

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

    while True:
        time.sleep(60)


if __name__ == "__main__":
    main()
