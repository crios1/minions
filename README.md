> **Status:** Pre-alpha (`0.0.x`). Core is active today; Compose and Cluster are roadmap direction. APIs and docs may change.

<p align="center">
  <img src="docs/_static/mascot-856.png" width="256" alt="Minions mascot">
</p>

# Minions

Minions is a progressive execution platform for Python workflow-per-event compute.

Build locally and progressively scale from single-process execution, to isolated containers, to distributed self-hosted clusters without rewriting your workflow code.

## The execution ladder

- **Minions Core** -> single-process runtime for local workflow execution.
- **Minions Compose** -> local containerized runtime topology for isolation and cloud-like deployment.
- **Minions Cluster** -> self-hosted distributed deployment of the Compose topology across multiple machines.

The workflow model stays stable while the execution envelope changes.

## What Minions runs

Minions is for systems that react to events, keep workflow state, and run business logic in ordered steps:

- bots, scrapers, automations, and controllers
- stream or queue consumers
- event-driven data processors
- workflow engines embedded in Python applications

You model the system directly:

- **Pipelines** produce events from sources such as WebSockets, queues, cron jobs, APIs, or local loops.
- **Minions** process each event through ordered workflow steps.
- **Context** stores per-workflow state that can be resumed.
- **Resources** provide shared dependencies such as clients, pools, caches, stores, and sidecars.
- **Gru** owns runtime lifecycle, dependency wiring, metrics, persistence, and shutdown.

## What stays stable

The Core/Compose/Cluster contract is about preserving workflow source code, not freezing deployment configuration.

As you move between execution modes, the same Python workflow should keep the same shape:

- event types and pipeline boundaries
- minion classes and ordered `@minion_step` methods
- context models and workflow state semantics
- resource dependencies declared in Python
- runtime-managed lifecycle, persistence, and metrics

What changes is the execution envelope around that workflow: process isolation, container packaging, machine placement, resource endpoints, persistence backends, and operator configuration.

## Platform support

Production deployments: Linux (systemd or containers). Windows is supported for local development and testing only when using WSL2.

## A minimal Minions example

Below is a small example that shows the shape of a Minions system. A pipeline produces events, a minion processes them in ordered steps, and context is shared across the workflow.

```python
import asyncio
from dataclasses import dataclass

from minions import Gru, Minion, Pipeline, minion_step


@dataclass
class MyEvent:
    greeting: str = "hello world"


class MyPipeline(Pipeline[MyEvent]):
    async def produce_event(self):
        return MyEvent()


@dataclass
class MyContext:
    last_greeting: str | None = None


class MyMinion(Minion[MyEvent, MyContext]):
    @minion_step
    async def step_1(self):
        self.context.last_greeting = self.event.greeting

    @minion_step
    async def step_2(self):
        print(self.context.last_greeting)


async def main():
    gru = await Gru.create()
    await gru.start_orchestration(MyPipeline, MyMinion)


if __name__ == "__main__":
    asyncio.run(main())
```

## Installation

The package name is reserved on PyPI, but the project is still pre-alpha.

To experiment locally:

```bash
pip install minions
```
