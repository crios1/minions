# Overview

Minions is a progressive execution platform for Python workflow-per-event compute. You define **pipelines** that emit events, **minions** that react to them, **context** that carries workflow state, and **resources** those minions share. `Gru` wires everything together, persists workflow state, and tears components down in dependency order. The project is pre-alpha (`0.0.x`): expect APIs and docs to evolve.

## Why this exists

- You want to build a workflow locally before committing to a final deployment topology.
- You want event pipelines, ordered workflow steps, typed context, resources, metrics, and persistence as Python runtime concepts.
- You want a path from single-process execution to isolated containers and self-hosted clusters without redesigning workflow code.

## Core ideas

- **Progressive execution**: the workflow model is designed to remain stable across Core, Compose, and Cluster execution modes ({doc}`execution-ladder`).
- **Workflow-per-event compute**: each pipeline event is processed by minions through ordered, stateful steps.
- **Explicit lifecycles**: every pipeline/minion/resource has `startup`, `run`, and `shutdown` hooks managed by the framework.
- **Typed events and contexts**: minions declare the event type they consume and the workflow context they mutate; both must be dataclasses or `msgspec.Struct` types with serializable fields.
- **Resource graph**: dependencies are inferred from type hints so Gru can start/stop once and inject safely.
- **Greedy concurrency**: the runtime pushes as much work as possible; backpressure lives in your resources ({ref}`concurrency-backpressure`).

## Gru in Minions Core

In Minions Core, `Gru` is a process-level runtime owner, not just a helper object.

- It owns lifecycle coordination for all running minions, pipelines, and resources in the process.
- It owns process-wide runtime services (metrics endpoint, persistence backend, background monitoring).
- A single owner keeps startup/shutdown behavior deterministic and avoids conflicting defaults (for example: one metrics port, one default SQLite file).

If you need multiple independent Core orchestrations at the same time, run multiple processes (one `Gru` per process). Compose and Cluster execution are the intended paths for stronger isolation or multi-machine deployment.

(runtime-component-sharing)=
## Runtime component sharing

Within one `Gru` process, `start_orchestration(...)` starts or joins a runtime composition. Each orchestration gets its own Minion instance, because the minion owns workflow execution for that minion/config/pipeline composition.

Pipelines and Resources are shared by identity:

- One Pipeline instance runs per pipeline identity.
- One Resource instance runs per resource identity.
- Additional orchestrations subscribe to the existing Pipeline and reuse existing Resources.
- Gru reference-counts ownership and stops shared components only after the last dependent orchestration is stopped.

This means `startup` for a Pipeline or Resource is not called once per orchestration. It is called once per running component identity in the process. Put per-orchestration state in Minion workflow context or config, not in shared Pipeline or Resource instance attributes.

```text
Gru process
|-- Pipeline: PriceFeedPipeline   shared by identity
|-- Resource: PriceAPI            shared by identity
|-- Orchestration A
|   `-- Minion instance for config A
|-- Orchestration B
|   `-- Minion instance for config B
`-- Orchestration C
    `-- Minion instance for config C
```

## Components

- **Gru** – orchestrator; manages lifecycles, dependency wiring, metrics, logging, and persistence.
- **Pipeline** – long-lived producer of events; Gru fans each event out to subscribed minions.
- **Minion** – worker that runs an ordered workflow of `@minion_step` methods per event.
- **Resource** – shared dependency with lifecycle hooks and automatic latency/error tracking.
- **StateStore/Logger/Metrics** – pluggable infrastructure interfaces (defaults: SQLite store, Prometheus metrics, file/no-op loggers).
