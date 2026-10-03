# Write an experiment node

Each node owns its Python runner, image, model, task and measurements. The orchestrator supplies environment values, launches containers, waits for completion and cleans up. The logger stores events and provides a startup barrier.

The example uses the shared DHT startup helper. Bootstrap seeds must print HIVEMIND_MADDR=<address> so the orchestrator obtains initial peer addresses. Implement your own startup helper if your experiment needs another approach. Averaging and topology capture helpers are optional.

## Runner example

```python
import os
import signal

from shared import client
from shared.dht import resolved_dht_id, start_dht

def main():
    """Join the experiment, record a custom measurement, and finish the task."""
    identity = {
        "experiment_id": os.environ["EXPERIMENT_ID"],
        "node_id": os.environ["NODE_ID"],
        "node_type": os.environ["NODE_TYPE"],
        "node_index": int(os.environ["NODE_INDEX"]),
    }
    endpoint = os.environ["LOGGER_ENDPOINT"]

    def shutdown(_signum, _frame):
        """Run cleanup when the container receives a stop signal."""
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)
    dht = start_dht()
    try:
        client.send_node_started(
            logger_endpoint=endpoint,
            **identity,
            hivemind_address=str(dht.get_visible_maddrs()[0]),
            dht_id=resolved_dht_id(dht),
        )
        client.wait_for_start_barrier(
            logger_endpoint=endpoint,
            experiment_id=identity["experiment_id"],
            node_id=identity["node_id"],
            timeout=180,
        )
        # Replace this measurement with the experiment's own work.
        client.send_event(
            logger_endpoint=endpoint,
            **identity,
            event_type="custom_measurement",
            data={"values": [1.0, 2.0], "description": "example"},
        )
        print("NODE_COMPLETE=1", flush=True)
    finally:
        dht.shutdown()

if __name__ == "__main__":
    main()
```

Save your runner as src/eclipse_hivemind/nodes/my-node/runner.py.

## Dockerfile example

```dockerfile
FROM python:3.12-slim
ENV PYTHONUNBUFFERED=1
WORKDIR /app
RUN pip install --no-cache-dir "hivemind==1.1.12"
COPY src/eclipse_hivemind/nodes/shared /app/shared
COPY src/eclipse_hivemind/nodes/my-node/runner.py /app/runner.py
CMD ["python", "/app/runner.py"]
```

Save your Dockerfile as docker/node-my-node.Dockerfile. Build your node and logger from the repository root:

```bash
docker build -f docker/node-my-node.Dockerfile -t my-node:dev .
docker build -f docker/logger.Dockerfile -t eclipse-hivemind-logger:dev .
```

## Configuration example

```yaml
schema_version: 1
experiment:
  name: my-experiment
  seed: 42
  rounds: 1
  node_timeout_seconds: 300
logger:
  endpoint: http://logger:8080
bootstrap:
  seeds:
    node_type: my-node
    count: 1
  policy: seed_only
node_types:
  my-node:
    count: 2
    image: my-node:dev
    parameters:
      example_value: 7
```

Run your experiment:

```bash
python -m eclipse_hivemind.cli --config-path <config.yaml> --outputs outputs
```

Parameters become PARAM_<UPPERCASE_NAME> environment variables. Your runner chooses how to use `EXPERIMENT_SEED` and `NODE_SEED`. `INITIAL_PEERS` contains space-separated bootstrap addresses. `ROUNDS` defines your task limit. node_timeout_seconds bounds the wait for each completion marker.

Print `NODE_COMPLETE=1` after your task and final reports succeed. Exit without the marker when the task fails. The orchestrator waits for all nodes before cleanup and attempts to save container logs after stopping containers.

Your run directory contains `events.jsonl`, `config.json` and container logs. The orchestrator announces the run ID before launching containers. Write your own analysis script for your event fields.

Logging requests run synchronously. Request failures raise to your runner. Choose whether a logging failure stops your task. The client performs no automatic retries.

When you configure startup phases, include each active node type once. Place the bootstrap seed type in the first phase.
