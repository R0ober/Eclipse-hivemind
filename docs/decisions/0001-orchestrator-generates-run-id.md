# Generate the experiment run identifier in the orchestrator, not the config
 
## The Problem
 
Two runs of the same configuration file must not collide in the aggregator, but a
config file is a reusable, static artifact.
 
## Options Considered
 
- A `run_id` field in the YAML.
- **The orchestrator generates a unique `run_id` at each launch.**
- Each node derives its own id from `name` + timestamp at startup.

## Rationale
 
A config file may be run more than once, so putting a fixed identifier in the file would cause the same identifier to be used again. This could mix data from different experiments, which we want to avoid.

Instead, we generate the identifier once for each run and use the same identifier for all nodes in that run. This also allows experiment.name to stay as a human-readable name that can be shared by related runs.

We did not choose to create a separate identifier for each node because that would give each node a different identifier, even though they are part of the same run.
 
## Notes
 
- `run_id` is also used as the results directory name and as the DHT/averager
  key prefix, so a single run is isolated end to end.
- Format is an orchestrator concern (e.g. `run-<date>-<short-random>` or a ULID),
  not part of this decision.
- Status: Accepted.
 
