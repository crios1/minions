# Overview

Minions is a single-process, event-driven runtime. Instead of spreading work across microservices, you define **pipelines** that emit events, **minions** that react to them, and **resources** those minions share. `Gru` wires everything together, persists workflow state, and tears components down in dependency order. The project is pre-alpha (`0.0.x`): expect APIs and docs to evolve.

## Why this exists

- You want orchestration, metrics, and persistence without Kubernetes, a queue, or multiple deploys.
- You have long-lived async workers—bots, scrapers, automations—that should be coordinated but not distributed.
- You still want safety: structured logs, Prometheus metrics, state checkpoints, and clear lifecycles.

## Core ideas

- **Single-process orchestration**: one event loop, many long-running services.
- **Explicit lifecycles**: every pipeline/minion/resource has `startup`, `run`, and `shutdown` hooks managed by the framework.
- **Typed events and contexts**: minions declare the event type they consume and the workflow context they mutate; both must be JSON-serializable structured types.
- **Resource graph**: dependencies are inferred from type hints so Gru can start/stop once and inject safely.
- **Greedy concurrency**: the runtime pushes as much work as possible; backpressure lives in your resources ({ref}`concurrency-backpressure`).

## Why single Gru per process

`Gru` is a process-level runtime owner, not just a helper object.

- It owns lifecycle coordination for all running minions, pipelines, and resources in the process.
- It owns process-wide runtime services (metrics endpoint, persistence backend, background monitoring).
- A single owner keeps startup/shutdown behavior deterministic and avoids conflicting defaults (for example: one metrics port, one default SQLite file).

If you need multiple independent orchestrations at the same time, run multiple processes (one `Gru` per process).

## Components

- **Gru** – orchestrator; manages lifecycles, dependency wiring, metrics, logging, and persistence.
- **Pipeline** – long-lived producer of events; Gru fans each event out to subscribed minions.
- **Minion** – worker that runs an ordered workflow of `@minion_step` methods per event.
- **Resource** – shared dependency with lifecycle hooks and automatic latency/error tracking.
- **StateStore/Logger/Metrics** – pluggable infrastructure interfaces (defaults: SQLite store, Prometheus metrics, file/no-op loggers).

## What this is not

- A distributed task queue or message broker.
- A replacement for Celery/Temporal/Kafka when you truly need horizontal scale.
- A stable 1.0 API (yet).
