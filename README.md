# Eclipse Hivemind

Experiments for studying eclipse attacks in Hivemind networks.

## Build the node image

The node image is built from the repository root, because the Dockerfile copies the
runner out of `src/`. The tag must match the `image` field in the config you run.

```bash
docker build -f docker/node.Dockerfile -t eclipse-hivemind-node:dev .
```

The first build installs torch and hivemind, so expect it to take a while.

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


check docker events to debug container issues 
```bash
docker events \
  --filter type=container \
  --filter event=create \
  --filter event=start \
  --filter event=destroy \
  --format '{{.Time}} {{.Action}} {{.Actor.Attributes.name}}'

```
