# Experiment configuration reference

Use your YAML config to select node images, counts, startup order and the logger endpoint. The parser supports `schema_version: 1`. Each runner owns its task and interprets its own parameters.

## Top-level structure

Place experiment-wide settings under `experiment`. Place node-specific settings under `node_types.<type>.parameters`. The orchestrator forwards node parameters as environment variables.

```yaml
schema_version: 1

experiment:
  name: my-experiment
  seed: 42
  rounds: 20
  node_timeout_seconds: 3600

logger:
  endpoint: http://logger:8080

bootstrap:
  seeds:
    node_type: normal
    count: 1
  policy: seed_only

node_types:
  normal:
    count: 3
    image: eclipse-hivemind-node-honest:dev
    parameters:
      batch_size: 32
      target_batch_size: 192
```

## Configuration fields

### `schema_version`

| Type | Required | Supported value |
| --- | --- | --- |
| Integer | Yes | `1` |

### `experiment`

These settings apply to the run. The orchestrator supplies the seed and task limit to each node.

| Field | Type | Required | Default | Purpose |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Non-empty experiment label. The orchestrator generates a separate run ID. |
| `seed` | Integer | Yes | None | Source for `EXPERIMENT_SEED` and each derived `NODE_SEED`. Your runner chooses how to use the seeds. |
| `rounds` | Integer | Yes | None | Positive task limit supplied as `ROUNDS`. Training runners use an epoch limit. The observer uses a snapshot count. |
| `node_timeout_seconds` | Number | No | `3600` | Maximum wait for each `NODE_COMPLETE=1` marker. Must be finite and positive. |

### `logger`

| Field | Type | Required | Default | Purpose |
| --- | --- | --- | --- | --- |
| `endpoint` | Address string | Yes | None | Logger address from inside the Docker network. Use `http://logger:8080` with the supplied service. |

The parser accepts URLs and IP addresses. The supplied HTTP client expects a URL, including the scheme and port.

### `node_types`

Use your type names as keys. The orchestrator assigns node IDs such as `normal-0` and `adversarial-0` from those names. At least one type needs a positive count.

| Field under `node_types.<type>` | Type | Required | Default | Purpose |
| --- | --- | --- | --- | --- |
| `count` | Integer | Yes | None | Number of containers. Must be zero or greater. |
| `image` | String | Yes | None | Non-empty Docker image reference. Build the image before launching your experiment. |
| `parameters` | Map | No | `{}` | Settings interpreted by this node's runner. The orchestrator converts values to strings. |

For example, this parameter belongs to `normal` nodes:

```yaml
node_types:
  normal:
    count: 3
    image: eclipse-hivemind-node-honest:dev
    parameters:
      batch_size: 32
```

The runner reads the parameter through `os.environ["PARAM_BATCH_SIZE"]`. Use the same pattern for your own settings. The logger and config parser do not interpret experiment-specific parameter names.

### `bootstrap`

Bootstrap settings select the nodes supplying initial peer addresses.

| Field | Type | Required | Default | Purpose |
| --- | --- | --- | --- | --- |
| `seeds.node_type` | String | With multiple types | The only configured type | Selects the type providing seed nodes. |
| `seeds.count` | Integer | No | `1` | Must be at least one and must not exceed the selected type's count. |
| `policy` | String | No | `seed_only` | Starts seeds first and passes their addresses to other nodes. The parser recognises `full`, but the orchestrator rejects this unimplemented policy. |

```yaml
bootstrap:
  seeds:
    node_type: normal
    count: 1
  policy: seed_only
```

The orchestrator reads each seed's `HIVEMIND_MADDR` output. Other nodes receive the seed addresses through `INITIAL_PEERS`.

With one configured type, omit `bootstrap` to use one seed from this type. With multiple types, supply `bootstrap.seeds.node_type` explicitly.

### `startup.phases`

Use phases when your experiment needs different arrival times. Without phases, the orchestrator launches all remaining nodes after the seeds and assigns one startup group.

| Field in each phase | Type | Required | Default | Purpose |
| --- | --- | --- | --- | --- |
| `name` | String | Yes | None | Non-empty, unique phase name. Also identifies the startup group. |
| `node_types` | List of strings | Yes | None | Non-empty list of configured type names. Each active type must appear once. |
| `wait_after_seconds` | Integer | No | `0` | Delay after launching the phase. Must be zero or greater. |

```yaml
startup:
  phases:
    - name: honest-network
      node_types: [normal]
      wait_after_seconds: 120
    - name: adversarial-nodes
      node_types: [adversarial]
```

This example launches honest nodes, waits 120 seconds, then launches adversarial nodes. The seed type must belong to the first phase because seeds always launch first. The orchestrator also applies a configured delay after the final phase.

Nodes using the logger's startup barrier wait for their own phase. They do not wait for later phases. Your runner chooses whether to use the barrier and how to handle a timeout.

## Parameters read by the supplied runners

All parameters in this section belong under `node_types.<type>.parameters`. They are optional runner settings, rather than new top-level fields.

### Honest and reverse-gradient training nodes

Both supplied training runners read these settings:

| Parameter | Default | Purpose and constraints |
| --- | --- | --- |
| `batch_size` | `32` | Positive number of samples per local step. |
| `learning_rate` | `0.05` | SGD learning rate. |
| `target_batch_size` | `total_nodes * batch_size * 2` | Swarm-wide sample target. Use the same value across averaging participants. Choose a target larger than one local batch for joint rounds. |
| `matchmaking_time` | `15` | Positive time in seconds for assembling a group. |
| `averaging_timeout` | `60` | Averaging timeout in seconds. Must exceed `matchmaking_time`. |
| `step_delay_seconds` | `1` | Delay after each local step. Must be zero or greater. The delay gives peers time to exchange progress on the supplied classifier task. |
| `target_group_size` | `0` | Zero leaves the choice to hivemind. A configured non-zero value must be at least two. |
| `min_group_size` | `0` | Zero leaves the choice to hivemind. A configured non-zero value must be at least two and must not exceed a configured `target_group_size`. |
| `eval_size` | `4096` | Positive number of held-out samples generated from `experiment.seed`. |
| `start_barrier_timeout` | `180` | Time in seconds for startup coordination. The supplied training runners report `node_error` and continue training after a barrier timeout. |

For exact-size groups, set `target_group_size` and `min_group_size` equally. A round with too few members falls back to local gradients. See [ADR 0016](decisions/0016-strict-averaging-group-size.md).

The reverse-gradient runner implements the attack directly. This runner does not read `strategy` or `target_prefix`.

### DHT settings shared by the supplied node types

| Parameter | Default | Purpose |
| --- | --- | --- |
| `dht_id_source` | Unset | Derives a deterministic DHTID from your source string. Without a source, hivemind selects a random ID. Use distinct sources for distinct DHTIDs. |

```yaml
node_types:
  normal:
    count: 1
    image: eclipse-hivemind-node-honest:dev
    parameters:
      dht_id_source: target-peer
```

A type's parameters apply to all nodes of the type. Use one node per type or implement per-node derivation in your runner when assigning distinct deterministic IDs.

### Observer nodes

| Parameter | Default | Purpose and constraints |
| --- | --- | --- |
| `snapshot_interval_seconds` | `1` | Positive delay in seconds between snapshot reports. |
| `target_dht_id` | Unset | Optional nearest-peer lookup target. Supply a 40-character hexadecimal DHTID. |
| `k_nearest` | `20` | Positive number of requested nearest peers. |
| `start_barrier_timeout` | `180` | Time in seconds for startup coordination. The observer exits on a barrier timeout. |

Without `target_dht_id`, the observer records its routing table without a nearest-target lookup. Use [configs/dht-observer.yaml](../configs/dht-observer.yaml) for a complete example.

## Complete training example

This example places each setting under its owning section. Both node types use the same averaging settings. The images select honest training or gradient reversal.

```yaml
schema_version: 1

experiment:
  name: gradient-reversal-example
  seed: 7
  rounds: 20
  node_timeout_seconds: 3600

logger:
  endpoint: http://logger:8080

bootstrap:
  seeds:
    node_type: normal
    count: 1
  policy: seed_only

startup:
  phases:
    - name: all-peers
      node_types: [normal, adversarial]

node_types:
  normal:
    count: 7
    image: eclipse-hivemind-node-honest:dev
    parameters:
      batch_size: 32
      learning_rate: 0.05
      target_batch_size: 1280
      matchmaking_time: 10
      averaging_timeout: 30
      step_delay_seconds: 1
      target_group_size: 5
      min_group_size: 5
      eval_size: 4096
      start_barrier_timeout: 180

  adversarial:
    count: 3
    image: eclipse-adversary-reverse-gradient:dev
    parameters:
      batch_size: 32
      learning_rate: 0.05
      target_batch_size: 1280
      matchmaking_time: 10
      averaging_timeout: 30
      step_delay_seconds: 1
      target_group_size: 5
      min_group_size: 5
      eval_size: 4096
      start_barrier_timeout: 180
```

## Minimal baseline example

With one type, bootstrap defaults select one seed and the `seed_only` policy. The runner uses its default training parameters.

```yaml
schema_version: 1

experiment:
  name: baseline-averaging
  seed: 1
  rounds: 50

logger:
  endpoint: http://logger:8080

node_types:
  normal:
    count: 3
    image: eclipse-hivemind-node-honest:dev
```

## Environment passed to each node

The orchestrator generates these values. Your YAML supplies the source settings, rather than individual node identities.

| Environment variable | Source or example |
| --- | --- |
| `EXPERIMENT_ID` | Generated run ID. |
| `EXPERIMENT_NAME` | `experiment.name`. |
| `EXPERIMENT_SEED` | `experiment.seed`. |
| `ROUNDS` | `experiment.rounds`. |
| `EXPERIMENT_TOTAL_NODES` | Sum of configured counts. |
| `NODE_ID` | Type and zero-based index, such as `normal-0`. |
| `NODE_TYPE` | Type name from `node_types`. |
| `NODE_INDEX` | Zero-based index within the type. |
| `NODE_SEED` | SHA-256 derivation from `experiment.seed` and `node_id`. |
| `LOGGER_ENDPOINT` | `logger.endpoint`. |
| `INITIAL_PEERS` | Space-separated seed multiaddresses. |
| `PARAM_<UPPERCASE_KEY>` | String value from `node_types.<type>.parameters.<key>`. |

`build_node_env` accepts an optional `identity_path` argument. The current orchestrator does not supply this argument or generate PeerID files. The shared DHT helper lets hivemind create the network identity.

## Validation and outputs

The parser checks required fields, value types, positive rounds, finite positive node timeouts, node counts, seed selection and phase coverage. The selected seed type must contain enough nodes. Phase references must name configured types.

The top-level config rejects unknown fields. Nested models do not enforce the same rejection rule. Parameter names remain experiment-defined. Keep spelling aligned with your runner.

The orchestrator announces `RUN_ID` before launching containers. Each run has its own Docker network. The supplied training runners build their averaging prefix from the run ID and classifier version.

The default output parent is `outputs/`. Use `--outputs` on the CLI or sweep script to select another directory. Each run stores `events.jsonl`, `config.json` and container logs. `config.json` records the parsed config, including defaults.

CPU and memory limits, subnet placement and additional bootstrap policies need implementation before use. Keep your parameter sweeps in experiment-owned tooling.
