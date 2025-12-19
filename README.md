> **Status:** Pre-alpha (`0.0.x`). APIs and docs are still evolving and may change without notice.

<p align="center">
  <img src="docs/_static/mascot-856.png" width="256" alt="Minions mascot">
</p>

# Minions

Get the benefits of microservices without the complexity of running a distributed system.

Minions is a single-process, Python-native runtime that coordinates your system’s long-lived components.

Define your components as Python classes, declare your orchestrations, and the runtime handles everything else—state, lifecycle, and resumability.

## Why Minions instead of microservices?

- One process, one deploy — no containers, queues, gRPC, or distributed ops
- Pure Python development with no distributed debugging or infra burden
- Built-in orchestration, lifecycle, dependency management, metrics, and state
- Safe, dependency-aware restarts and redeployment of individual components
- Fully portable: a Minions system is just a project folder.  
  Copy it anywhere and the entire orchestration — code, configs, and state — runs exactly the same.

If you don’t need container-grade isolation or horizontal scaling, Minions is almost always simpler, faster, and more efficient.  
Minions also lets you isolate risky or unstable code so that failures in those parts never bring down the runtime.

<!-- ## Minions vs Microservices (side-by-side) -->
<!-- 
    TODO:
    I imagine this section to be a list examples that prove
    how much better minions is than microservices in terms of
    how much it costs to operate, how much less time and effort
    it is to operate, etc. start first with reference examples
    and in the future i can be testimonials of users almost
-->
## Microservices vs Minions

Imagine a system that:

- Listens to a stream of external events (WebSocket, queue, cron, etc.)
- Pulls in additional data for each event
- Applies business logic and takes action when conditions are met

This could be an on-chain trading bot, an IoT controller, a real-time data processor, or any event-driven system with long-lived state and operational complexity.

### A typical microservices setup

A common microservice-style design for a system like this might look like:

- event-listener-service (subscribes to events and pushes them to a queue)
- data-service (HTTP/gRPC API for fetching additional data)
- worker-service (consumes events, calls data services, applies logic)
- Shared state in Redis/Postgres for coordination and limits
- Message broker (Kafka/RabbitMQ/etc.) for fan-out and buffering
- Containers + orchestrator (Docker/Kubernetes/etc.) for each service
- CI/CD pipelines and deployment scripts for every component
- Centralized logging and metrics to reconstruct system behavior

This works, but it comes with familiar costs:

- Multiple deployable units to build, version, and operate
- Network boundaries and failure modes between every step
- Cross-service coordination and compatibility concerns
- Distributed debugging when something goes wrong

### The same system with Minions

Minions keeps the shape of a microservice system, but collapses it into a single, structured runtime.

Instead of decomposing the system across processes and networks, you model the system directly:

- Pipelines → event sources (WebSocket listeners, queue consumers, cron jobs)
- Resources → shared services (DB clients, HTTP clients, price oracles)
- Minions → long-lived workers that apply business logic
- Minion steps → ordered stages in a workflow
- Context → per-workflow state (what you’d otherwise persist or pass between services)
- Gru → the orchestrator (lifecycle, wiring, metrics, shutdown)

The result is a single process with explicit structure, lifecycle management, and observability — without queues, containers, or distributed coordination.

### A minimal Minions example

Below is a small example that shows the shape of a Minions system.
A pipeline produces events, a minion processes them in ordered steps, and context is shared across the workflow.

```python
import asyncio
from dataclasses import dataclass
from typing import Any

from minions import Minion, Pipeline, Gru, minion_step

@dataclass
class MyEvent:
    greeting: str = "hello world"

class MyPipeline(Pipeline[MyEvent]):
    async def produce_event(self):
        return MyEvent()

@dataclass
class MyContext:
    last_greeting: Any = None

class MyMinion(Minion[MyEvent, MyContext]):
    @minion_step
    async def step_1(self):
        self.context.last_greeting = self.event.greeting

    @minion_step
    async def step_2(self):
        print(self.context.last_greeting)

async def main():
    gru = await Gru.create()
    await gru.start_minion(MyMinion, MyPipeline)

if __name__ == "__main__":
    asyncio.run(main())
```

You still get:

- Clear separation of concerns
- Long-lived workers and shared dependencies
- Explicit workflow structure and lifecycle management

But you only:

- Deploy one process
- Debug one runtime
- Operate one system

Need more throughput? Start with `docs/guides/scale-out-strategies.md`. If you later outgrow the single-process model, see `docs/guides/migrating-to-microservices.md`.

## Installation

The package name is reserved on PyPI, but the project is **not** ready for general use.

If you still want to experiment at your own risk:

```bash
pip install minions
