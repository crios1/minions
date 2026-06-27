```{toctree}
:maxdepth: 2
:hidden:

getting-started
how-to/run-your-first-minion
how-to/writing-a-custom-resource
how-to/integrating-with-cli
reference/cli
reference/api/minions_core
reference/api/minions_cli
guides/patterns-and-anti-patterns
guides/testing-minions
guides/operating-with-metrics
guides/updating-minions
guides/deployment-strategies
guides/migrating-to-microservices
guides/scale-out-strategies
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Concepts

concepts/overview
concepts/execution-ladder
concepts/minions
concepts/pipelines
concepts/resources
concepts/state-and-persistence
concepts/concurrency-and-backpressure
concepts/gru-lifecycle-failure-management
concepts/startup-forms
concepts/project-structure
concepts/portability
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Advanced

advanced/sidecar-resources
advanced/minimal-installs
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: kitchen skin

kitchen-skin/need-a-nonnative-library
kitchen-skin/contributing
kitchen-skin/writing-minions
```

# Minions

Minions is a progressive execution platform for Python workflow-per-event compute.

Build locally and progressively scale from single-process execution, to isolated containers, to distributed self-hosted clusters without rewriting your workflow code.

## The execution ladder

Minions has three execution layers: Core for single-process workflow execution, Compose for local containerized topology, and Cluster for self-hosted multi-machine deployment.

See {doc}`/concepts/execution-ladder` for the compatibility contract between layers.

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

### A minimal Minions example

Below is a small example that shows the shape of a Minions system.
A pipeline produces events, a minion processes them in ordered steps, and context is shared across the workflow.

```python
import asyncio
from dataclasses import dataclass

from minions import Minion, Pipeline, Gru, minion_step

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

## Next steps

- Start with {doc}`getting-started` for a concrete “hello minion” walkthrough
- Read {doc}`/concepts/overview` to understand the Minions mental model
- Jump to {doc}`how-to/run-your-first-minion` for a hands-on checklist
- Use {doc}`/guides/operating-with-metrics` when you need to interpret persistence and runtime telemetry in production
- Explore {doc}`reference/api/minions_core` as the API surface evolves
