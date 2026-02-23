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

pipeline = HeartbeatPipeline  # module-level export helps Gru resolve it
```

Guidelines from the runtime:

- Declare the **event type** via generics; it must be JSON-serializable and structured (not bare primitives).
- Implement `produce_event`, an infinite (or very long-lived) async producer.
- Optionally implement `startup`, `run`, and `shutdown` hooks inherited from `AsyncService`.
- Expose a module-level `pipeline` variable or a single `Pipeline` subclass so Gru can resolve it from the module path you pass to `start_minion`.

## Resources and fanout

Pipelines can declare resource dependencies via type hints just like minions. Gru starts those resources first, injects them, and reuses them across subscribers.
Events are shared fanout inputs: fanout delivers the same event object to each subscribed minion, prefer immutable event types (like frozen dataclasses) and store mutable per-workflow state in context.

Each produced event increments `PIPELINE_EVENT_PRODUCED_TOTAL`. Fanout to minions increments `PIPELINE_EVENT_FANOUT_TOTAL` and is logged in debug mode for traceability.
