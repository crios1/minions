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
    @minion_step
    async def reserve_inventory(self):
        ...

    @minion_step
    async def charge_customer(self):
        ...
```

Rules from the runtime:

- Declare **event** and **workflow context** types via generics. Both must be JSON-serializable structured types (dataclasses/TypedDicts are fine; bare primitives are rejected).
- Bare `dict` is accepted for quick examples and prototypes. For durable workflow state, prefer explicit schemas such as dataclasses, TypedDicts, msgspec structs, or `dict[str, V]` / `Mapping[str, V]` with serializable values.
- Steps must be instance methods decorated with `{py:func}``@minion_step``. They run in source order.
- Use `self.event` to access the current pipeline event; the event is contextvar-bound per workflow.
- Use `self.context` to read and update the current workflow context; steps do not receive it as an argument.
- Use `self.workflow_handle` when business code needs optional diagnostic correlation data for logs or audit records. The read-only handle exposes `orchestration_id` and `workflow_id`, matching the stable identity fields used by framework diagnostics and persisted workflow state.
- Raise `{py:class}``minions._internal._domain.exceptions.AbortWorkflow`` to stop a workflow gracefully without treating it as a failure.
- Do not raise `asyncio.CancelledError` to intentionally stop a workflow. The runtime treats cancellation as an interruption, keeps the persisted workflow context, and may replay the workflow later. Use `AbortWorkflow` when the workflow should stop as an intentional terminal outcome.

### Workflow handle

`self.workflow_handle` is available only while a workflow is running. Accessing it outside an active workflow raises `RuntimeError` because there is no current workflow identity to report.

Use it to copy runtime identity into business-owned records without coupling those records to Minions internals:

```python
class OrderMinion(Minion[OrderEvent, WorkflowCtx]):
    audit_log: AuditLog

    @minion_step
    async def charge_customer(self):
        handle = self.workflow_handle
        await self.audit_log.record(
            action="charge_customer",
            order_id=self.event.order_id,
            orchestration_id=handle.orchestration_id,
            workflow_id=handle.workflow_id,
        )
```

Framework diagnostics and business audit trails remain separate concerns: Minions logs and metrics describe runtime behavior, while your audit records describe domain work. The handle is just a narrow bridge for correlation when that is useful during debugging or investigations.

### Reserved attribute space

Names starting with `_mn_` are reserved for the runtime across minions, pipelines, and resources. Do not define attributes or annotations with that prefix; Gru rejects classes that collide with the reserved “minions attr-space.”

## Resources inside minions

Dependencies are declared via type hints. Gru inspects hints, starts resources, and injects them on the minion before it runs.

```python
from .resources import PriceAPI

class PriceMinion(Minion[PriceEvent, WorkflowCtx]):
    price_api: PriceAPI  # injected by Gru

    @minion_step
    async def fetch_price(self):
        self.context.price = await self.price_api.get_price(self.context.symbol)
```

Resources can depend on other resources; Gru reference-counts the graph and shuts down unused nodes when a minion stops.

## Lifecycles and observability

Minions inherit `startup`, `run`, and `shutdown` hooks from `AsyncService`. Gru:

- starts each minion as a task and waits for `startup`
- logs workflow/step start, success, abort, and failure (with user file/line when available)
- emits Prometheus counters/gauges/histograms for workflows and steps
- cancels outstanding workflows during shutdown and drains the state store
