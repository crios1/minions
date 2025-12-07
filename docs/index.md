# Minions

Async-native orchestration framework for long-lived minions (workers) that run inside a single Python process. Minions gives you microservice-like structure—pipelines, resources, metrics, persistence—without the infra sprawl.

```{note}
Status: pre-alpha (`0.0.x`). APIs and naming may change.
```

- Single-process orchestration with pipelines, minions, and shared resources
- Structured lifecycle hooks with logging, metrics, and persistence baked in
- Resource graph management so dependencies start/stop in the right order
- Pragmatic defaults (SQLite state store, Prometheus metrics, file/no-op loggers) but all pluggable

```{toctree}
:maxdepth: 2
:hidden:

getting-started
concepts/overview
concepts/minions
concepts/pipelines
concepts/resources
concepts/state-and-persistence
concepts/concurrency-and-backpressure
how-to/run-your-first-minion
how-to/writing-a-custom-resource
how-to/integrating-with-cli
reference/cli
reference/api/minions_core
reference/api/minions_cli
guides/patterns-and-anti-patterns
guides/testing-minions
guides/deployment-strategies
```

## Who this is for

Developers who want to coordinate multiple async workers—bots, scrapers, trading agents, background automation—without spinning up full microservice stacks.

## How to read this

- Start with {doc}`getting-started` for a concrete “hello minion” walkthrough.
- Deep-dive into the mental model in {doc}`concepts/overview`.
- Jump to {doc}`how-to/run-your-first-minion` when you want a task-focused checklist.
- See {doc}`reference/api/minions_core` for generated API docs as the surface evolves.
