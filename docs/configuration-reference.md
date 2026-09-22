# Experiment configuration reference

This document describes the YAML file for a single experiment. It defines the configuration used by the configuration parser and orchestrator. The node type labels exported by the orchestrator are also used in the experiment results.

This document is **living documentation**. It changes when `schema_version` changes. The reasoning behind non-obvious configuration choices is documented separately in dated decision records under `decisions/`. This keeps the reference current without changing the history of those decisions.

* Current `schema_version`: **1**

---

## Top-level structure

```yaml
schema_version: 1

experiment:
  name: adversarial-ratio-30
  seed: 42
  rounds: 50

aggregator:
  endpoint: http://aggregator:8080

startup:
  phases:
    - name: honest-network
      node_types:
        - normal
      wait_after_seconds: 120
    - name: adversarial-nodes
      node_types:
        - adversarial

bootstrap:
  seeds:
    node_type: normal
    count: 1
  policy: seed_only

node_types:
  normal:
    count: 7
    image: hivemind-node:latest
  adversarial:
    count: 3
    image: hivemind-node:latest
    parameters:
      strategy: eclipse
      target_prefix: "expert."
```

---

## Fields

### `schema_version` (int, required)

Version of the configuration schema.

The value must match a schema version supported by the parser. The current version is `1`.

This field allows future schema changes to be detected instead of being silently interpreted using the wrong format.

### `experiment` (map, required)

| Field    | Type   | Required | Default | Notes                                                                                                         |
| -------- | ------ | -------- | ------- | ------------------------------------------------------------------------------------------------------------- |
| `name`   | string | yes      | —       | Human-readable label used to group related runs. **Not** the run identifier — see ADR 0001.                   |
| `seed`   | int    | yes      | —       | Single reproducibility setting. Each node derives its own seed as `hash(seed, node_id)` — see ADR 0005.       |
| `rounds` | int    | yes      | —       | Number of rounds before the experiment ends. One averager step equals one round. Must be `> 0`. See ADR 0006. |

### `aggregator` (map, required)

| Field      | Type   | Required | Default | Notes                                                                                                                               |
| ---------- | ------ | -------- | ------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| `endpoint` | string | yes      | —       | URL where nodes submit telemetry. The URL is resolved through Docker DNS from each container, for example `http://aggregator:8080`. |

### `startup` (map, optional)

Controls the order in which node types are started. The startup plan is kept
separate from `node_types` so node behavior and experiment timing can change
independently.

#### `startup.phases` (list, optional)

Each phase starts the listed node types, then waits for the configured delay
before the next phase. If `startup` is omitted, the phase list is empty.

A phase is also the cohort its nodes synchronise with: the nodes of one phase
wait for each other at the aggregator's start barrier before their first step,
and do not wait for later phases. That is what lets a later phase join a swarm
that is already training. See ADR 0012.

| Field | Type | Required | Default | Notes |
| ----- | ---- | -------- | ------- | ----- |
| `name` | string | yes | — | Human-readable phase name. |
| `node_types` | list of strings | yes | — | Names from the top-level `node_types` map. |
| `wait_after_seconds` | int | no | `0` | Delay after the phase before the next phase. Must be `>= 0`. |

For example, this starts the honest nodes first, waits two minutes, and then
starts the adversarial nodes:

```yaml
startup:
  phases:
    - name: honest-network
      node_types: [normal]
      wait_after_seconds: 120
    - name: adversarial-nodes
      node_types: [adversarial]
```

### `bootstrap` (map, optional)

Controls how much of the peer set each node knows when it starts.

This is an experiment variable, not implementation plumbing — see ADR 0002.

| Field             | Type   | Required | Default             | Notes                                                                                                                                              |
| ----------------- | ------ | -------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `seeds.node_type` | string | no       | the only node type* | Node type from which the seed peers are selected. Must reference an existing node type.                                                            |
| `seeds.count`     | int    | no       | `1`                 | Number of seed peers. Must be `>= 1` and `<=` the count of `seeds.node_type`.                                                                      |
| `policy`          | enum   | no       | `seed_only`         | `seed_only`: non-seed nodes start knowing only the seed(s), and discovery finds the remaining peers. `full`: every node starts knowing every peer. The current orchestrator supports `seed_only`; `full` is reserved for a future implementation. |

* If `bootstrap` is omitted, the defaults are used: one seed from the only node type. If there are multiple node types, the first honest type is used. The default policy is `seed_only`.

For eclipse runs, `seed_only` is required for the attack to be observable — see ADR 0002. The current orchestrator rejects `full` until it can precompute all peer addresses before startup.

### `node_types` (map, required, at least one entry)

A map keyed by **type name**.

Each key must be unique. The type name is also the exact identifier used later for labelling, storage, and exporting results.

| Field        | Type   | Required | Default | Notes                                                                                                                                                                      |
| ------------ | ------ | -------- | ------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `count`      | int    | yes      | —       | Number of containers of this type. Must be `>= 0`. At least one node type must have a count `> 0`.                                                                         |
| `image`      | string | yes      | —       | Docker image reference, such as `repo/name:tag` or a digest. Using the same image for multiple types is normal. Behaviour is selected through `parameters` — see ADR 0004. |
| `parameters` | map    | no       | `{}`    | Free-form key/value parameters that control node behaviour. Each parameter is passed to the container as a `PARAM_<UPPERCASE_KEY>` environment variable.                   |

#### Common parameters for a training node type

These are read by the node runners, not by the parser, so they follow the
`PARAM_*` contract like any other parameter.

| Parameter | Default | Meaning |
| --------- | ------- | ------- |
| `batch_size` | `32` | Samples per local step. |
| `learning_rate` | `0.05` | SGD learning rate. |
| `target_batch_size` | `total_nodes x batch_size x 2` | Samples the swarm must accumulate before an epoch ends. One epoch is one aggregation round, so this has to be larger than one peer's batch; otherwise every peer finishes an epoch alone on every step and `round` equals `step`. **Every node type in a run must use the same value**, or the peers disagree about when an epoch ends. |
| `matchmaking_time` | `15` | Seconds hivemind spends assembling an averaging group. Needs room for every peer to join. |
| `averaging_timeout` | `60` | Seconds for the all-reduce. Must be greater than `matchmaking_time`; the node refuses to start otherwise, because hivemind schedules the round `matchmaking_time` ahead and then asserts that it fits inside the timeout. |
| `step_delay_seconds` | `1.0` | Sleep after each local step. A step on the toy model takes about a millisecond, so without this a peer reaches `target_batch_size` on its own before hivemind's progress tracker has fetched anyone else's progress, and no real aggregation ever happens. It stands in for the compute time of a realistic step. |
| `eval_size` | `4096` | Size of the held-out evaluation set, generated from `experiment.seed` so every node scores the same samples. |
| `start_barrier_timeout` | `180` | Seconds a node waits at the start barrier for the other nodes **in its own startup phase** to report `node_started`. It does not wait for later phases, so it is unaffected by `wait_after_seconds`. On a timeout the node trains anyway and reports a `node_error`. |

#### Common parameters for an adversarial type

| Parameter       | Meaning                                                                                             |
| --------------- | --------------------------------------------------------------------------------------------------- |
| `strategy`      | Attack strategy used by the node, for example `eclipse`.                                            |
| `target_prefix` | Target of the attack. The node uses this value to derive its DHTID placement — see ADR 0003 / 0005. |

---

## Validation rules

The parser enforces the following rules:

* `schema_version` must be a supported version.
* All required fields must be present and have the correct type.
* `rounds` must be `> 0`.
* Every `count` must be `>= 0`.
* At least one node type must have `count > 0`.
* `bootstrap.seeds.node_type` must exist in `node_types`.
* `bootstrap.seeds.node_type` must have a `count` greater than or equal to `bootstrap.seeds.count`.
* Every `startup.phases[].node_types[]` entry must exist in `node_types`.
* Every `startup.phases[].wait_after_seconds` value must be `>= 0`.
* `aggregator.endpoint` must be a valid URL.
* **Unknown fields are rejected**, not ignored. The configuration uses strict decoding, so a typo such as `conut: 7` fails instead of being silently ignored.
* Validation errors are collected and reported together. Each error includes the path to the invalid field instead of stopping at the first error.

---

## What the config does *not* contain

The following values are generated or derived by the orchestrator when the experiment starts. They are intentionally not part of the YAML file.

| Concern                                              | Where it comes from                                                                                                                              |
| ---------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `run_id`                                             | Generated for each launch. It is also used as the results directory name and the DHT/averager prefix. See ADR 0001.                              |
| `node_id`                                            | Generated as `"{type}-{index}"`, where `index` starts at 0 within each type.                                                                     |
| Per-node seed                                        | Derived as `hash(experiment.seed, node_id)`. See ADR 0005.                                                                                       |
| Per-node DHTID                                       | Honest nodes use a random DHTID. Adversarial nodes derive it from `strategy` + `target_prefix`. See ADR 0003.                                    |
| Per-node PeerID / identity                           | The orchestrator generates one `identity_path` for each container so all maddrs are known before startup.                                        |
| Per-container `initial_peers`                        | Built from the `bootstrap` policy and the known PeerIDs.                                                                                         |
| DHT/averager prefix                                  | Set to `run_id`. Each run is also isolated using a separate Docker network.                                                                      |
| Listen address / port, network name, container names | Set by orchestrator constants. `host_maddrs` listens on `0.0.0.0:<port>`. The bridge-routable maddr is captured instead of the loopback address. |

---

## Deliberately deferred

Add these settings only when there is a need to vary them.

When added, they should be placed under `experiment.parameters` if they apply to the whole experiment rather than to individual nodes.

* Averager tuning: `target_group_size`, `min_group_size`, `averaging_alpha`.
* Per-type `resources`, such as CPU and memory limits.
* Per-type `subnet` / `network`. These are only relevant if an IP-diversity defense is added. Hivemind's Kademlia DHT does not currently use one — see ADR 0003.
* Parameter sweeps. A separate sweep file that generates multiple resolved configs is cleaner than using list-valued fields in this file.

---

## Environment passed to each container

For reference, this is the contract between the orchestrator and each container:

```text
EXPERIMENT_ID        run-2026-09-15-a3f9   # = run_id
EXPERIMENT_NAME      adversarial-ratio-30
EXPERIMENT_SEED      42
ROUNDS               50
EXPERIMENT_TOTAL_NODES 10                  # every node counts the swarm it is sizing rounds for
NODE_ID              adversarial-0
NODE_TYPE            adversarial
NODE_INDEX           0
NODE_SEED            <derived>
IDENTITY_PATH        /identities/adversarial-0.id
AGGREGATOR_ENDPOINT  http://aggregator:8080
INITIAL_PEERS        <space-separated maddrs>
PARAM_STRATEGY       eclipse               # one PARAM_* per parameters key
PARAM_TARGET_PREFIX  expert.
```

---

## Examples

### Minimal — plain averaging baseline

This example relies on the defaults. With `bootstrap` omitted, the configuration uses one seed and the `seed_only` policy.

```yaml
schema_version: 1

experiment:
  name: baseline-averaging
  seed: 1
  rounds: 50

aggregator:
  endpoint: http://aggregator:8080

node_types:
  normal:
    count: 3
    image: hivemind-node:latest
```

### Full — every field exercised

```yaml
schema_version: 1

experiment:
  name: adversarial-ratio-30      # human label; groups related runs. NOT the run id.
  seed: 42                        # single knob; each node derives hash(seed, node_id)
  rounds: 50                      # termination: one averager step = one round

aggregator:
  endpoint: http://aggregator:8080   # resolved via Docker DNS from every node

startup:
  phases:
    - name: honest-network
      node_types: [normal]
      wait_after_seconds: 120         # delay before adversarial nodes start
    - name: adversarial-nodes
      node_types: [adversarial]

bootstrap:
  seeds:
    node_type: normal             # entry point(s) drawn from an honest type
    count: 1
  policy: seed_only               # discovery fills the rest; required to observe eclipse
                                  # alt: full → every node starts with every peer

node_types:
  normal:
    count: 7
    image: hivemind-node:latest
  adversarial:
    count: 3
    image: hivemind-node:latest   # same image; behaviour selected by parameters
    parameters:
      strategy: eclipse
      target_prefix: "expert."    # attack target; node derives its DHTID source from this
```
