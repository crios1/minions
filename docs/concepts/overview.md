# Overview

Minions is a Python-native runtime for long-running, event-driven systems. You
define **pipelines** that emit events, **minions** that react to them,
**context** that carries workflow state, and **resources** those minions share.
`Gru` runs them together in one Python process, persists workflow state, and
tears components down in dependency order. The project is pre-alpha (`0.0.x`):
expect APIs and docs to evolve.

## Why this exists

- You want to model and run an event-driven system directly in Python.
- You want event pipelines, ordered workflow steps, typed context, resources, metrics, and persistence as Python runtime concepts.
- You want one runtime to own component lifecycles, dependency wiring, workflow persistence, and shutdown.

## Key ideas

- **Single-process ownership**: one `Gru` coordinates the runtime components and workflows in its Python process.
- **Workflow-per-event compute**: each pipeline event is processed by minions through ordered, stateful steps.
- **Explicit lifecycles**: the framework manages startup, active execution, and
  shutdown for every pipeline, minion, and resource.
- **Typed events and contexts**: minions declare the event type they consume and the workflow context they mutate; both must be dataclasses or `msgspec.Struct` types with serializable fields.
- **Resource graph**: dependencies are inferred from type hints so Gru can start/stop once and inject safely.
- **Greedy concurrency**: the runtime pushes as much work as possible; backpressure lives in your resources ({ref}`concurrency-backpressure`).

## Gru in Minions

In Minions, `Gru` is a process-level runtime owner, not just a helper object.

- It owns lifecycle coordination for all running minions, pipelines, and resources in the process.
- It owns process-wide runtime services (metrics endpoint, persistence backend, background monitoring).
- A single owner keeps startup/shutdown behavior deterministic and avoids conflicting defaults (for example: one metrics port, one default SQLite file).

If you need multiple independent Minions runtimes, run one `Gru` per process
and provide any required partitioning or cross-process coordination outside
Minions.

(runtime-component-sharing)=
## Runtime component sharing

Within one `Gru` process, `start_orchestration(...)` starts or joins a runtime composition. Each orchestration gets its own Minion instance, because the minion owns workflow execution for that minion/config/pipeline composition.

Pipelines and Resources are shared by identity:

- One Pipeline instance runs per pipeline identity.
- One Resource instance runs per resource identity.
- Additional orchestrations subscribe to the existing Pipeline and reuse existing Resources.
- Gru reference-counts ownership and stops shared components only after the last dependent orchestration is stopped.

This means `startup` for a Pipeline or Resource is not called once per orchestration. It is called once per running component identity in the process. Put per-workflow state in Minion workflow context, and do not put orchestration-specific state in shared Pipeline or Resource instances.

```text
Gru process
|-- Pipeline: PriceFeedPipeline   shared by identity
|-- Resource: PriceAPI            shared by identity
|-- Orchestration A
|   `-- Minion instance for config A
|-- Orchestration B
|   `-- Minion instance for config B
`-- Orchestration C
    `-- Minion instance for config C
```

(component-state-ownership)=
## Component state ownership

Choose where state lives according to the lifetime and sharing the state needs:

| State location | Lifetime and sharing |
| --- | --- |
| Class attribute | Belongs to the Python class object and is shared across current and future instances of that class. Gru does not reset it when a component instance stops. |
| Minion instance attribute | Belongs to one orchestration's Minion instance and is shared by that Minion's concurrent workflows. |
| Pipeline instance attribute | Belongs to one running Pipeline identity and is shared by all subscribing orchestrations. |
| Resource instance attribute | Belongs to one running Resource identity and is shared by all current dependents. |
| `self.context` | Belongs to one workflow created for one event and is persisted for interruption and resume. |

Initialize mutable component-lifecycle state on `self`, normally in `startup`.
Although Gru keeps at most one live Pipeline or Resource instance per identity,
it can stop that instance after its last consumer leaves and create another
instance of the same class later. Instance state follows that managed lifecycle;
mutable class state does not.

An annotation written in a component class without an assigned value declares
an instance attribute; it does not create a class-level value. Gru inspects
supported dependency and configuration annotations and binds the resolved value
to each component instance:

```python
class PriceMinion(Minion[PriceEvent, PriceContext]):
    prices: PriceAPI  # Gru assigns self.prices
```

Use `ClassVar` only when a value intentionally belongs to the component type
rather than any managed instance:

```python
from typing import ClassVar

class PriceMinion(Minion[PriceEvent, PriceContext]):
    category: ClassVar[str] = "pricing"
```

`ClassVar` communicates ownership to readers and type checkers; it does not make
the value immutable at runtime. Prefer immutable values for user-defined class
attributes. Keep mutable per-workflow Minion state in `self.context`, not on the
Minion instance, so concurrent workflows remain isolated and resumable.

## Runtime roles

- **Gru** – orchestrator; manages lifecycles, dependency wiring, metrics, logging, and persistence.
- **Pipeline** – long-lived producer of events; Gru fans each event out to subscribed minions.
- **Minion** – worker that runs an ordered workflow of `@minion_step` methods per event.
- **Resource** – shared dependency with lifecycle hooks and automatic latency/error tracking.
- **StateStore/Logger/Metrics** – pluggable infrastructure interfaces (defaults: SQLite store, Prometheus metrics, file/no-op loggers).
