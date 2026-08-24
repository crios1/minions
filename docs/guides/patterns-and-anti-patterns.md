# Patterns and Anti-Patterns

A few guardrails while the framework is still evolving.

## Patterns

- **Explicit resources**: move I/O and rate limits into `Resource` subclasses; keep minion steps mostly orchestration logic.
- **Typed events/contexts**: use dataclasses or `msgspec.Struct` types with serializable fields. If serialization throughput matters, prefer `msgspec.Struct` (or use `@dataclass(slots=True)` as a close second).
- **Idempotent steps**: design steps so reruns after restarts are safe; persist intent, not transient state.
- **Compose via type hints**: declare dependencies with annotations instead of manual wiring; let Gru manage the graph.
- **Metrics-first**: use the built-in Prometheus counters/gauges to watch throughput and errors before tuning concurrency.

## Component inheritance

User-authored Minion, Pipeline, Resource, Logger, Metrics, and StateStore
classes must declare exactly one direct Python base. Additional bases and
mixins are rejected. Use helper functions, helper objects, composition, or a
Resource to share behavior across otherwise independent component classes.
User-defined metaclasses are also unsupported for Minions components.

Ordinary single-inheritance specialization remains supported where the
component family permits it. For example, a StateStore implementation may
specialize another StateStore implementation without adding a second base.
Minion and Pipeline retain stricter construction rules: subclass Minion or
Pipeline directly; a completed Minion or Pipeline subclass cannot be extended
further.

## Anti-patterns

- **Global throttling**: do not add arbitrary sleeps in minion steps to slow the world; throttle inside the resource that needs it.
- **Opaque contexts**: avoid stuffing huge payloads or non-serializable objects into workflow context.
- **Multiple minion subclasses per module without `minion` alias**: Gru will refuse to start if it cannot disambiguate.
- **Long blocking work in steps**: keep steps async-friendly; move blocking work to threads or subprocesses.
- **Ignoring config validation**: override `load_config` to fail fast when misconfigured.
