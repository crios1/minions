# Execution Ladder

Minions is designed around a progressive execution ladder: start with the smallest runtime that fits, then add isolation or distribution when the workflow needs it.

The ladder has three layers:

- **Minions Core** -> single-process runtime for local workflow execution.
- **Minions Compose** -> local containerized runtime topology for isolation and cloud-like deployment.
- **Minions Cluster** -> self-hosted distributed deployment of the Compose topology across multiple machines.

The ladder is about changing the execution envelope around a workflow, not changing the workflow model itself.

## The stable workflow model

Across the ladder, the same Python workflow should keep the same shape:

- event types and pipeline boundaries
- minion classes and ordered `@minion_step` methods
- context models and workflow state semantics
- resource dependencies declared in Python
- runtime-managed lifecycle, persistence, and metrics

This is the core compatibility promise: Minions should preserve workflow source code as the runtime grows from local execution toward more isolated or distributed topologies.

## What changes between layers

Deployment configuration is allowed to change between Core, Compose, and Cluster.

Common changes include:

- process isolation
- container packaging
- machine placement
- resource endpoints
- persistence backends
- operator configuration
- secrets and environment wiring
- runtime supervision

These are execution concerns. They should not require redesigning the workflow's event types, minion steps, context model, or resource dependency shape.

## Minions Core

Core runs a Minions workflow inside a single Python process owned by `Gru`.

Use Core when you want the fastest local feedback loop, direct Python composition, simple supervision, or one runtime per deployment unit.

Core is also the foundation for learning and testing the workflow model. See {doc}`startup-forms` for how Core loads workflow components into `Gru`.

## Minions Compose

Compose is the intended path for running a Minions topology with container isolation on one machine.

Use Compose when the workflow model still fits, but you want stronger dependency isolation, cloud-like local deployment, separate resource envelopes, or a topology that is closer to production operations.

Compose should preserve the workflow source code contract while moving more operational detail into container and topology configuration.

## Minions Cluster

Cluster is the intended path for distributing the Compose topology across multiple machines under your control.

Use Cluster when the workflow model still fits, but you need multiple hosts, stronger placement control, or self-hosted distributed deployment of the same topology.

Cluster should make distribution explicit without forcing workflow code to become conventional microservice glue.

## When to leave the ladder

If your requirements exceed the Minions workflow contract, you may choose to migrate to conventional microservices or another orchestration system.

Good reasons include platform requirements outside the Minions model, organizational ownership boundaries, unsupported coordination needs, or a need for mature hosted/distributed workflow features today.

See {doc}`/guides/migrating-to-microservices` for a pragmatic mapping out of Minions.
