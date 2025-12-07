# Overview

Minions is a single-process, event-driven orchestration framework. Instead of scattering work across microservices, you define **pipelines** that emit events, **minions** that react to them, and **resources** those minions share. `Gru` is the runtime that wires everything together, monitors health, and tears components down in the right order.

## Why this exists

- You want structure, metrics, and persistence without running Kubernetes or a message bus.
- You have lots of long-lived async work—bots, scrapers, automation loops—that should be coordinated but not distributed.
- You still want observability and safety: structured logs, Prometheus metrics, state checkpoints, and backpressure primitives.

## Core ideas

- **Single-process orchestration**: one event loop, many long-running services.
- **Explicit lifecycles**: every pipeline/minion/resource has `startup`, `run`, and `shutdown` hooks managed by the framework.
- **Typed events and contexts**: Minions declare the event type they consume and the workflow context they mutate.
- **Resource graph**: dependencies are inferred from type hints so Gru can start/stop them once and share them safely.
- **Greedy concurrency**: the runtime pushes as much work as possible; rate limits live inside your resources ({ref}`concurrency-backpressure`).

## Components

- **Gru** – the orchestrator. Manages lifecycles, dependency wiring, metrics, logging, and persistence.
- **Pipeline** – long-lived producer of events. Events are fanned out to subscribed minions.
- **Minion** – worker that reacts to each event by running an ordered workflow (its `@minion_step` methods).
- **Resource** – shared dependency with tracking wrappers and lifecycle hooks (e.g., HTTP clients, DB pools).
- **StateStore/Logger/Metrics** – pluggable infrastructure interfaces. Defaults: SQLite + file logger + Prometheus exporter, with no-op fallbacks.

## What this is not

- A distributed task queue or message broker.
- A replacement for Celery/Temporal/Kafka when you truly need horizontal scale.
- A stable 1.0 API (yet).

Keep those constraints in mind when deciding whether to deploy Minions or reach for heavier tooling.
