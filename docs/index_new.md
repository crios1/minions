# Minions

Minions is a Python-native runtime for systems that have outgrown a script, but don’t yet justify the complexity of microservices.

It provides a single-process, local-first execution model where long-lived components are explicitly orchestrated, supervised, and persisted.

You define your components as Python classes, declare how they interact, and Minions handles lifecycle, state, dependency ordering, and resumability—without requiring a distributed system.

## Why Minions instead of microservices?

Many systems don’t start distributed—they *become* distributed.

Minions is designed for the phase where:
- a single script is no longer sufficient,
- orchestration and lifecycle concerns are becoming real,
- but container orchestration and microservices would add unnecessary cost and complexity.

Minions lets you model your system explicitly while keeping everything local, observable, and easy to reason about end-to-end.

### What Minions gives you

- One process, one deploy — no containers, queues, gRPC, or distributed ops
- Pure Python development with no distributed debugging burden
- Built-in orchestration, lifecycle management, dependency ordering, metrics, and state
- Safe, dependency-aware restarts and redeployment of individual components
- Fully portable systems: a Minions project folder contains the full orchestration—code, configuration, and state—and runs the same anywhere

If you don’t need container-grade isolation or horizontal scaling, Minions is often simpler, faster, and more efficient than a distributed architecture.

### Scaling and migration

Minions is not a dead end.

When you need more throughput, you can incrementally scale out by externalizing selected components while continuing to use Minions for orchestration (see {doc}`/guides/scale-out-strategies`).

If your requirements eventually demand a fully distributed system, Minions components map cleanly to microservices boundaries, making migration straightforward (see {doc}`/guides/migrating-to-microservices`).

The goal is not to avoid microservices forever—it’s to adopt them deliberately, if and when they are truly warranted.

<!-- todo: continue like the original index.md -->
