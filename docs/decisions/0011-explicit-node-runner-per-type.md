# Give each node type its own explicit runner, sharing only genuinely common code

## The Problem

ADR 0004 chose one image per experiment, with node behaviour selected at
runtime through `PARAM_*` variables. That works while there is one behaviour
switch. It stops working once node types diverge in what they actually do:
reading "what does an adversarial node do" then means reading one runner file
that branches on parameters, instead of reading one file that only describes
that node type.

We expect to add more node types over time. We want adding one to stay easy to
reason about, both for us now and for someone reading the code later.

## Options Considered

- Keep one runner and grow the parameter-driven dispatch as behaviour is added.
- Add a plugin/strategy registry inside the node package, so a runner looks up
  behaviour by name and calls into it.
- **Give each node type its own runner file and its own image, sharing only
  code that is identical across all types.**

## Rationale

A registry keeps one image, which was the point of ADR 0004, but it replaces a
parameter switch with a dispatch table. That is still one file you have to
read to understand any single node type, and it still hides the actual
behaviour behind a layer of indirection.

An explicit runner per type means a node type is just a normal Python script.
Understanding the adversarial node means opening its runner and reading it.
There is nothing to look up and nothing shared except what is actually the
same for every type: talking to the aggregator, and starting the DHT.

The configuration already supports this. `node_types[].image` has always been
allowed to differ per type — ADR 0004 chose to point every type at the same
image, but the schema never required that. This change uses the flexibility
that was already there.

Building one image per type costs more build time and more Dockerfiles than a
single image. We accept that cost because it is paid once per type, not once
per experiment, and because the alternative cost — a harder-to-read runner as
node types grow — is paid every time someone reads the code.

## Decision

- `src/eclipse_hivemind/node/shared/` holds code identical across every node
  type: the aggregator HTTP client and the DHT bootstrap/readiness helper.
- Each node type gets its own folder with its own `runner.py`, for example
  `src/eclipse_hivemind/node/honest/runner.py`.
- Each node type gets its own Dockerfile under `docker/`, copying `shared/`
  plus that type's `runner.py`.
- `node_types[].image` in the experiment config points at the image built for
  that type.

## Notes

- `parameters` still exists for tuning within a type, for example
  `target_prefix` for the adversarial node's target. It no longer needs to
  select which code path runs — that is now the image.
- This raises the cost of adding a node type: a new folder, a new Dockerfile,
  a new build. That is intentional. A node type is a real code path, not a
  configuration value.
- `shared/` should stay small. If a helper starts encoding behaviour decisions
  instead of plumbing, it belongs in the runner that needs it, not in
  `shared/`.
- Status: Accepted.
