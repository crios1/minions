# Minions

Minions are async workers that react to events produced by a pipeline. Each event spawns a **workflow**: an ordered series of `@minion_step` methods. Workflows carry a **context** object that survives across steps and can be persisted.

## Anatomy of a Minion

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

Key rules:

- Declare **event** and **workflow context** types via generics. Both must be JSON-serializable (dataclasses and TypedDicts are fine; bare primitives are rejected).
- Steps must be instance methods decorated with `{py:func}``@minion_step``. They run in source order.
- Use `self.event` inside steps to access the pipeline event that triggered the workflow.
- Raise `{py:class}``minions.exceptions.AbortWorkflow`` to stop a workflow gracefully without treating it as a failure.

## Resources inside minions

Dependencies are declared via type hints. Gru inspects those hints, starts the resources, and injects instances on the minion before it runs.

```python
from .resources import PriceAPI

class PriceMinion(Minion[PriceEvent, WorkflowCtx]):
    price_api: PriceAPI  # injected by Gru

    @minion_step
    async def fetch_price(self, ctx: WorkflowCtx):
        ctx.price = await self.price_api.get_price(ctx.symbol)
```

Resources can also depend on other resources; the graph is started once and reference-counted.

## Lifecycles

Minions inherit lifecycle hooks from `AsyncService`:

- `startup` – initialize derived state or validate config.
- `run` – optional background loop for long-lived tasks.
- `shutdown` – cleanup; called even when startup fails.

Gru starts minions as tasks, waits for startup to finish, and handles shutdown when you stop a minion or the process exits.
