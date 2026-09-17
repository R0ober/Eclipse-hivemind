# Measure the node-count ceiling on a single host before sizing experiments

## The Problem

ADR 0003 commits to one container per node on a single Docker bridge, but it does
not say how many nodes that supports. We pick experiment sizes without knowing the
limit, so we do not know whether a result is shaped by the attack or by the host
running out of room.

## Options Considered

- Assume the sizes we use today are fine and find the limit by hitting it.
- **Run a scaling test up to about 1000 nodes and record where it stops working**.
- Move to a multi-host setup now and avoid the question.

## Rationale

We do not know what one node costs. The development host has 15 GB of memory, so
1000 nodes would need to fit in about 15 MB each. Bare CPython already uses more
than that before Hivemind is imported. The honest expectation is that 1000 nodes
does not work on one host, and the useful output of the test is the number that
does work and what runs out first.

This matters for the eclipse experiments specifically. The ratio of adversarial to
honest nodes is the variable we care about, and a low ceiling limits which ratios we
can test at a believable network size. Knowing the ceiling tells us whether we can
study the attack at scale or only in miniature.

We did not choose to move to multiple hosts first. That would contradict ADR 0003
without evidence that a single host is insufficient, and it adds work we may not
need.

## What we will measure

- Resident memory per node, once the DHT is up and idle.
- Wall-clock time to start N nodes, for N at several points up to the ceiling.
- Time until every node has discovered peers, and whether it happens at all.
- Open file descriptors and threads per node.
- The first resource to run out: memory, CPU, file descriptors, or startup time.

## Notes

- The node image installs torch, but the DHT runtime does not use it. A DHT-only
  image may cost far less per node. Measure both, because the cheaper image may
  raise the ceiling enough to matter.
- The orchestrator starts containers one at a time and waits for each seed to log
  its address. Startup time may bind before memory does. If so, starting containers
  concurrently is the cheaper fix.
- Hivemind runs a separate libp2p daemon process per node, so the cost per node is
  not one process.
- If the ceiling is far below the sizes we want, the options are a lighter image,
  several DHT nodes per container, or multiple hosts. Only the last one contradicts
  ADR 0003.
- This record is a research question, not a settled choice. It becomes Accepted when
  the measurements exist and we have chosen a working size.
- Status: Proposed.
