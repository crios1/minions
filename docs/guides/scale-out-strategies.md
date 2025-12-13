# Scale-Out Strategies

Minions is intentionally a **single-process runtime**. The default way to handle more load is to **scale up** (bigger box, more concurrency) and keep the system simple.

When scale-up isn’t enough, “scale-out” with Minions is mostly about **topology**:

- replicate the runtime into multiple processes, and
- ensure each process owns a **disjoint slice of work**, or offload hotspots to **sidecars**.

Minions does not (yet) provide cross-process coordination. If you run multiple runtimes with the same inputs, you’ll usually get duplicated work unless you partition ownership explicitly.

## 0) Start with scale-up (the normal path)

Scale-up preserves Minions’ simplest deployment model:

- Increase CPU/RAM.
- Increase safe concurrency (more workflows/steps in flight).
- Put backpressure at the edges (rate-limit inside resources), not by “sleeping” in steps.

Related reading: {doc}`concepts/concurrency-and-backpressure`, {doc}`guides/deployment-strategies`, {doc}`guides/patterns-and-anti-patterns`.

## 1) Offload hotspots to a sidecar process (recommended “scale-out”)

If you have CPU headroom on the host but one dependency is the bottleneck (or risky), move that dependency out of process:

- CPU-heavy work (compression, crypto, ML inference)
- risky native libs (segfault risk)
- slow/blocking integrations you want isolated

Your Minions process stays the orchestrator; the sidecar scales independently (more processes/threads; different resource limits).

Related reading: {doc}`advanced/sidecar-resources`.

## 2) Run multiple Minions runtimes with sharded ownership

You can run N identical runtimes (separate processes/containers/hosts) and scale out by making sure each instance owns different work.

### A) Shard at the ingress (pipelines)

Make pipelines publish work into an external transport that supports partitioning (queue/topic/stream). Then run multiple runtime replicas consuming partitions.

This keeps the Minions internal model intact while externalizing fan-out and ownership.

Typical patterns:

- **Partitioned stream**: instances consume different partitions (e.g., Kafka partitions).
- **Work queue**: instances pull jobs from a queue with at-least-once delivery.

The key is choosing a stable partition key (tenant/account/symbol/chain) so a workflow stays “owned” by one shard unless you explicitly design otherwise.

### B) Shard by component group

Split one big runtime into a few deployables:

- “hot path” minions in one runtime
- “cold path” minions in another
- ingress pipelines in their own runtime (optional)

This is still “single-process per deployable”, but you gain independent scaling/restarts per group without fully migrating to microservices.

## 3) Singleton responsibilities: leader/worker (active/standby)

Some responsibilities should be singleton (schedulers, reconcilers, periodic cleanup). If you run multiple runtime replicas, add a leader/lease mechanism so only one does the singleton work.

Common approaches (outside Minions):

- DB advisory locks / lease rows
- Redis-based leases
- platform leader election (Kubernetes)

Use followers for shard-local work only, or keep them hot as failover.

## 4) State and correctness in scale-out setups

Minions’ in-process orchestration is simple; distributed orchestration is not. If you scale out, explicitly decide how you preserve correctness:

- **Delivery semantics**: assume at-least-once and make side effects idempotent (see {doc}`kitchen-skin/writing-minions`).
- **Ownership**: ensure a workflow/event is handled by exactly one shard at a time (partitioning or leases).
- **Persistence**: decide where workflow state lives and how it’s updated safely (see {doc}`concepts/state-and-persistence`).
- **Replays**: define what happens on restart (re-consume events? resume from DB? dedupe?).

If you’re currently using SQLite for the state store, multiple replicas generally means either:

- one runtime per independent shard with its own local state file, or
- moving persistence to a shared external store that can support your ownership model.

## 5) When to stop scaling Minions and migrate

If you need independent scaling for many components, strong isolation boundaries, or lots of cross-process coordination, it may be time to migrate to microservices.

See {doc}`guides/migrating-to-microservices` for a pragmatic 1:1 mapping approach.

