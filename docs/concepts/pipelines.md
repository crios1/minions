# Pipelines

Pipelines are long-lived producers of events. Gru subscribes minions to pipelines, fans events out, and tracks metrics for production and fanout.

## Defining a pipeline

```python
import asyncio
from minions import Pipeline
from .types import Heartbeat

class HeartbeatPipeline(Pipeline[Heartbeat]):
    async def produce_event(self) -> Heartbeat:
        await asyncio.sleep(1)
        return Heartbeat(timestamp=time.time())

pipeline = HeartbeatPipeline
```

Guidelines:

- Declare the **event type** via generics. It must be JSON-serializable.
- Implement `produce_event`, an infinite (or very long-lived) async generator of events.
- Optionally implement `startup`, `run`, and `shutdown` hooks inherited from `AsyncService`.
- Expose a module-level `pipeline` variable or a single `Pipeline` subclass so Gru can resolve it from the module path you pass to `start_minion`.

## Resources and fanout

Pipelines can declare resource dependencies via type hints just like minions. Gru starts those resources first, injects them, and reuses them across subscribers.

Each produced event increments `PIPELINE_EVENT_PRODUCED_TOTAL`. Fanout to minions increments `PIPELINE_EVENT_FANOUT_TOTAL` and logs the dispatch in debug mode.
