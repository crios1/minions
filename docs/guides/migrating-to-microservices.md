# Migrating from Minions to Microservices

Minions is designed to keep your system **structured like microservices**, but deployed as **one Python process**. If you eventually need to move to a distributed architecture, the migration path is usually straightforward because your system is already decomposed into components with clear boundaries.

This guide explains how to map a Minions system to microservices, what you’ll need to replace when you leave the runtime, and a practical step-by-step way to migrate without a rewrite.

## The core idea: 1:1 component mapping

Most Minions systems already look like a microservice architecture on the inside:

- **Pipelines** define ingress (where events come from).
- **Minions** define long-lived workers that react to events and run workflows.
- **Resources** encapsulate dependencies (DBs, HTTP clients, queues, chain nodes, caches).
- **Orchestrations** define composition (which pipelines/minions run together).

When you migrate to microservices, you usually turn each of those into a deployable unit (or a small set of deployable units), while keeping the domain logic largely intact.

## When microservices are worth it

Minions is often the simplest choice if you don’t need container-grade isolation or horizontal scaling. Consider migrating when you genuinely need things a single process can’t deliver well:

- **Independent scaling** of hot components (e.g., many strategy workers, few ingress workers).
- **Hard isolation** (security, untrusted code, stricter blast-radius constraints).
- **Multi-language** components or heavy native dependencies you don’t want in one artifact.
- **Multi-team ownership** with independent release trains and SLAs.
- **Geographic distribution** or strong data-locality constraints.

If you mostly want “separation of concerns” and “operational clarity”, a Minions deployment mode system can often get you there without going distributed (see {doc}`concepts/runtime-modes` and {doc}`guides/deployment-strategies`).

If you’re primarily thinking about throughput, also consider the intermediate options in {doc}`guides/scale-out-strategies`.

## Mapping Minions concepts to microservices

This isn’t the only mapping, but it’s the most common starting point:

| Minions concept | Typical microservice equivalent |
| --- | --- |
| `Minion` | A worker service (consumes messages/events, runs workflow logic) |
| `Pipeline` | An ingress service (pulls/receives external events and publishes them) |
| `Resource` | A dependency adapter (DB/HTTP client wrapper) or a “shared library” module |
| `Gru` orchestration | Your platform glue (Kubernetes/systemd + queues + schedulers + service discovery) |
| State store / workflow context | A DB table, event store, or durable cache owned by the service |
| Minion steps / retries | Queue retries + idempotency keys + compensations in the service |

Minions makes step execution durable and resumable (see {doc}`concepts/state-and-persistence`). When you migrate, you’ll re-create those guarantees explicitly using your platform and data store choices.

## What tends to be “copy/paste”

If you kept your Minions code honest (domain logic in pure Python; I/O behind resources), you usually keep:

- The **domain model** and decision logic inside minion steps
- The **resource adapters** (often moved into a shared library, or vendored per service)
- The **configuration schema** (translated to env vars/config maps/secret stores)

What usually changes is the *outer shell* around that logic: networking, message transport, deployment, and the operational glue Minions previously provided.

## What you must replace when leaving Minions

Plan for these explicitly:

- **Event transport**: queue/topic/stream (Kafka, RabbitMQ, SQS, Redis streams, NATS, etc.)
- **Service lifecycle**: startup/shutdown, dependency wiring, health checks
- **Workflow persistence**: where workflow state lives and how it’s updated safely
- **Retries and backpressure**: retry policy, rate limits, dead-letter handling
- **Observability**: logs/metrics/traces across services

Minions already pushes you toward idempotent logic because step execution is at-least-once. Keep that discipline; it maps directly to microservices reliability patterns (see {doc}`kitchen-skin/writing-minions`).

## Migration strategies

### 1) “Strangler” migration (recommended)

Run Minions as the system of record while you peel off components into services one by one:

1. Pick one boundary (usually a single minion + its resources).
2. Extract its domain logic into a shared module (or keep it where it is and import it).
3. Build a service wrapper around it (HTTP or queue consumer).
4. Redirect traffic/events to the new service.
5. Delete (or disable) the old minion once stable.

This keeps risk low and lets you validate observability and correctness incrementally.

### 2) “Big bang” migration

Move everything at once only if the system is small, the team is experienced with distributed ops, and you can tolerate a larger integration/testing phase. It’s easy to accidentally lose reliability properties that Minions was giving you “for free”.

## Step-by-step: turning a minion into a service

### Step 1: Make the boundary explicit

Identify the minion’s inputs and outputs:

- Inputs: pipeline events, timers, scheduled triggers, RPC calls
- Outputs: DB writes, messages, HTTP calls, side effects

In microservices, make that boundary concrete as either:

- An **HTTP API** (request/response), or
- A **message consumer** (async processing)

### Step 2: Freeze the domain logic behind a function

Refactor (if needed) so your minion’s core decision logic is callable without the Minions runtime:

- Move “pure logic” into functions/classes that accept plain Python inputs
- Keep I/O in small adapters (the equivalent of resources)

### Step 3: Replace resources with service-local adapters

In Minions, `Resource` instances are dependency-injected and lifecycle-managed. In a service:

- Create the adapters in your app startup (DB pool, HTTP client, etc.)
- Pass them into your handler/worker
- Shut them down cleanly on termination

### Step 4: Choose a workflow persistence model

If the minion relied on resumable steps, pick one of:

- Store workflow state in a DB row keyed by workflow ID
- Use an event-sourced log (append events; rebuild state)
- Use a durable cache with explicit versioning/leases

Make idempotency explicit: use idempotency keys, unique constraints, and/or dedupe tables for side effects.

### Step 5: Swap orchestration for platform primitives

Replace “start/stop/restart” semantics with:

- Deployments (Kubernetes) or unit files (systemd)
- Readiness/liveness probes
- Horizontal scaling rules (if needed)
- Separate deployables per component boundary

## A concrete example mapping (from the README)

The “on-chain trading bot” example in `README.md` maps cleanly:

- `ChainEvents(Resource)` becomes an ingress service: `chain-events-ingress` publishes events to a topic.
- `PriceFeed(Resource)` becomes either:
  - a shared library used by workers, or
  - a dedicated `price-service` if you need independent scaling/caching.
- `OnChainStrategy(Minion)` becomes a worker service: `strategy-worker` consumes chain events and submits transactions.

Your `OnChainStrategy` logic stays Python; the migration work is mostly deciding the transport (HTTP vs queue) and explicitly implementing the reliability guarantees (state, retries, idempotency) that Minions previously handled in-process.

## Checklist: “done” looks like

- Each service has a clear interface (topic/API) and versioning story
- Workflow state is durable and updated atomically
- Retries are safe (idempotency keys/dedupe/unique constraints)
- Backpressure exists at the edges (rate limits, queue depth alarms)
- Metrics, logs, and traces let you debug cross-service flows
