# Minions

Minions are async workers that react to pipeline events. Each event spawns a **workflow**: an ordered series of `@minion_step` methods that share a **context** object. Gru persists context + step index before each step so workflows can resume after restarts; you still need to make steps idempotent.

## Anatomy of a minion

```python
from dataclasses import dataclass
from minions import Minion, minion_step

@dataclass
class WorkflowCtx:
    user_id: str
    retries: int = 0

class OrderMinion(Minion[dict, WorkflowCtx]):
    name = "order-minion"

    @minion_step
    async def reserve_inventory(self, ctx: WorkflowCtx):
        ...

    @minion_step
    async def charge_customer(self, ctx: WorkflowCtx):
        ...
```

Rules from the runtime:

- Declare **event** and **workflow context** types via generics. Both must be JSON-serializable structured types (dataclasses/TypedDicts are fine; bare primitives are rejected).
- Steps must be instance methods decorated with `{py:func}``@minion_step``. They run in source order.
- Use `self.event` to access the current pipeline event; the event is contextvar-bound per workflow.
- Raise `{py:class}``minions._internal._domain.exceptions.AbortWorkflow`` to stop a workflow gracefully without treating it as a failure.

## Resources inside minions

Dependencies are declared via type hints. Gru inspects hints, starts resources, and injects them on the minion before it runs.

```python
from .resources import PriceAPI

class PriceMinion(Minion[PriceEvent, WorkflowCtx]):
    price_api: PriceAPI  # injected by Gru

    @minion_step
    async def fetch_price(self, ctx: WorkflowCtx):
        ctx.price = await self.price_api.get_price(ctx.symbol)
```

Resources can depend on other resources; Gru reference-counts the graph and shuts down unused nodes when a minion stops.

## Lifecycles and observability

Minions inherit `startup`, `run`, and `shutdown` hooks from `AsyncService`. Gru:

- starts each minion as a task and waits for `startup`
- logs workflow/step start, success, abort, and failure (with user file/line when available)
- emits Prometheus counters/gauges/histograms for workflows and steps
- cancels outstanding workflows during shutdown and drains the state store
