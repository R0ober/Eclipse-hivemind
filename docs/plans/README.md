# Plans and recorded status

Decision records live in [../decisions/README](../decisions/README). These files describe study status and proposed work; a proposal does not establish attack success.

## Implemented measurement support

- Node identifier control and live DHTID reporting: see the [shared DHT helper](../../src/eclipse_hivemind/nodes/shared/dht.py).
- Averaging membership capture: see [ADR 0017](../decisions/0017-averaging-membership-from-optimizer-result.md).
- DHT topology logging: experiment-owned DHT runners and the observer collect routing observations separately from ordinary training runners.
- Startup coordination and logging: see [ADR 0012](../decisions/0012-logger-start-barrier.md) and the logger API documentation.

## Repeated-run study

The recorded collection contains 30 completed runs per configuration: 330 single-group runs and 180 smaller-group runs. Read [05-experiment-3.md](05-experiment-3.md) for status and [the analysis guide](../../analysis/experiment-3/README.md) for exported tables.

## Controlled-composition comparison

The [with-eclipse experiment](../../configs/experiment-2-with-eclipse/README.md) imposes gradient groups around a designated victim. It preserves shared progress tracking and state sharing. Its results assess imposed composition rather than a malicious mechanism for obtaining membership control.

## Leader-attack proposal

[06-eclipse-leader-attack.md](06-eclipse-leader-attack.md) describes proposed leadership and admission changes. Its acceptance criteria require separate implementation and verification. Do not treat the controlled-composition comparison as satisfying those criteria.
