# Patterns and Anti-Patterns

A few guardrails while the framework is still evolving.

## Patterns

- **Explicit resources**: move I/O and rate limits into `Resource` subclasses; keep minion steps mostly orchestration logic.
- **Structured events/contexts**: use dataclasses/TypedDicts instead of bare primitives to keep serialization predictable.
- **Idempotent steps**: design steps so reruns after restarts are safe; persist intent, not transient state.
- **Compose via type hints**: declare dependencies with annotations instead of manual wiring; let Gru manage the graph.
- **Metrics-first**: use the built-in Prometheus counters/gauges to watch throughput and errors before tuning concurrency.

## Anti-patterns

- **Global throttling**: do not add arbitrary sleeps in minion steps to slow the world; throttle inside the resource that needs it.
- **Opaque contexts**: avoid stuffing huge payloads or non-serializable objects into workflow context.
- **Multiple minion subclasses per module without `minion` alias**: Gru will refuse to start if it cannot disambiguate.
- **Long blocking work in steps**: keep steps async-friendly; move blocking work to threads or subprocesses.
- **Ignoring config validation**: override `load_config` to fail fast when misconfigured.
