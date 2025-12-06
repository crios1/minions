# minions

Async-native orchestration framework for running many long-lived “minions” (workers) on a single machine, with persistence and structured workflows.

> **Status:** Pre-alpha (`0.0.x`). APIs are experimental and may change without notice.

---

## What is this?

`minions` is an opinionated orchestration framework for:

- running multiple long-lived async workers (“minions”)
- wiring them into structured workflows / pipelines
- managing shared resources with explicit lifecycles
- persisting state (like via SQLite/aiosqlite) so workflows can resume after restarts

It’s aimed at things like:

- trading / bot fleets
- data collection loops
- background automation agents
- single-node orchestrated “systems” that don’t warrant full Kubernetes-style infra

The core ideas:

- **Pipelines** – long-lived event emitters that produce work items
- **Minions** – long-running async workers that listen to pipelines and spawn workflows
- **Workflows** – per-event units of work that run to completion using shared resources
- **Resources** – shared dependencies with startup/shutdown semantics
- **Persistence** – minimal, pragmatic state so the system can recover and resume


---

## Project status

This project is under active development and still in the **pre-0.1.0** design/implementation phase:

- APIs and naming are still being refined
- Tests and docs are evolving
- Breaking changes are expected between `0.0.x` versions

If you’re here early, treat the codebase as a workbench, not a stable library.

---

## Installation

The package name is reserved on PyPI, but the project is **not** ready for general use.

If you still want to experiment at your own risk:

```bash
pip install minions
