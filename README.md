# Eclipse Hivemind

Experiments for studying eclipse attacks in Hivemind networks.

## Build the node image

The honest node is the only node type implemented so far — see ADR 0011. Its
image is built from the repository root, because the Dockerfile copies
`shared/` and `honest/runner.py` out of `src/`. The tag must match the `image`
field in the config you run.

```bash
docker build -f docker/node-honest.Dockerfile -t eclipse-hivemind-node-honest:dev .
```

The first build installs torch and hivemind, so expect it to take a while.

## Build and run the aggregator

Build the aggregator image from the repository root:

```bash
docker build -f docker/aggregator.Dockerfile -t eclipse-hivemind-aggregator:dev .
```

## Validate a configuration

```bash
python3 -m eclipse_hivemind.cli --config-path configs/adversarial-ratio-30.yaml
```

When the project is installed in editable mode (using pip install -e .), the command-line entry point is:

```bash
eclipse-hivemind --config-path configs/adversarial-ratio-30.yaml
```

For Docker lifecycle testing before the Hivemind node image exists, use the
explicit Alpine test mode:

```bash
eclipse-hivemind --config-path configs/adversarial-ratio-30.yaml --fake-nodes
```

## Measure container resources

Measure one current DHT node for 30 seconds and write  JSON:

```bash
python scripts/measure_container_resources.py \
  --image eclipse-hivemind-node-honest:dev \
  --ready-pattern HIVEMIND_READY \
  --output measurements/dht-node.json
```

The same script can measure a future training node or aggregator by changing
`--image` and, when needed, `--command`. It records image size, startup time,
peak and average memory, CPU, process count, and raw samples. Run one
measurement per workload and keep the duration, interval, host, and image tag
with the report.

## Container logging 

check docker events to debug container issues 
```bash
docker events \
  --filter type=container \
  --filter event=create \
  --filter event=start \
  --filter event=destroy \
  --format '{{.Time}} {{.Action}} {{.Actor.Attributes.name}}'

```

Can also use to see prints from the network, run this in a new terminal after you have started a experiment.
```bash
bash scripts/check_docker_logs_for_experiment.sh 
``


