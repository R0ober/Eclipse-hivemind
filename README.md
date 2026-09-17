# Eclipse Hivemind

Experiments for studying eclipse attacks in Hivemind networks.

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
