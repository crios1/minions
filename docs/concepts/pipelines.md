# Pipelines

Pipelines are long-lived producers of events. Gru subscribes minions to pipelines, fans events out, and tracks production/fanout metrics.

## Defining a pipeline

```python
import asyncio, time
from minions import Pipeline
from .types import Heartbeat

class HeartbeatPipeline(Pipeline[Heartbeat]):
    async def produce_event(self) -> Heartbeat:
        await asyncio.sleep(1)
        return Heartbeat(timestamp=time.time())

```

Guidelines from the runtime:

- Declare the **event type** via generics; it must be a dataclass or `msgspec.Struct` type with serializable fields.
- Prefer explicit event schemas for durable systems.
- Implement `produce_event`; each call should produce and return one event.
- Optionally implement `startup` and `shutdown` hooks for setup and cleanup.
- Define a single `Pipeline` subclass in the module so Gru can resolve it from the module path you pass to `start_orchestration`.
- If a module contains multiple local pipeline classes, set a module-level `pipeline` variable to identify the entrypoint class.

## Resources and fanout

Within one `Gru` process, a pipeline identity maps to one running Pipeline instance shared by subscribed orchestrations; see {ref}`runtime-component-sharing` for the full orchestration sharing model.
Pipelines can declare resource dependencies via type hints just like minions. Gru starts those resources first, injects them, and reuses them across subscribers.
Events are shared fanout inputs: fanout delivers the same event object to each subscribed minion, prefer immutable event types (like frozen dataclasses) and store mutable per-workflow state in context.

Each produced event increments `PIPELINE_EVENT_PRODUCED_TOTAL`. Fanout to minions increments `PIPELINE_EVENT_FANOUT_TOTAL` and is logged in debug mode for traceability.

## Production failures

Gru repeatedly calls `produce_event()` while the pipeline is active. If one
call raises an ordinary exception, Minions logs and measures that failed
production attempt, emits no event for it, and continues by calling
`produce_event()` again.

Minions does not retry the failed call, reconstruct a lost event, or otherwise
recover user production logic. If repeated failures can occur immediately,
`produce_event()` should provide appropriate pacing so that it does not create a
tight error loop.
