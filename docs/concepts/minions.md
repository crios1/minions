# Minions

Minions are async workers that react to pipeline events. Each event spawns a **workflow**: an ordered series of `@minion_step` methods that share a **context** object. Gru persists context + step index before each step so workflows can resume after restarts; you still need to make steps idempotent.

## Anatomy of a minion

```python
from dataclasses import dataclass
from minions import Minion, minion_step

@dataclass(frozen=True)
class OrderEvent:
    user_id: str

@dataclass
class WorkflowCtx:
    user_id: str = ""
    retries: int = 0

class OrderMinion(Minion[OrderEvent, WorkflowCtx]):
    @minion_step
    async def reserve_inventory(self):
        ...

    @minion_step
    async def charge_customer(self):
        ...
```

Rules from the runtime:

- Declare **event** and **workflow context** types via generics. Both must be dataclasses or `msgspec.Struct` types with serializable fields.
- Workflow context types must be constructible without arguments. Minions
  creates one for each live event, so required context fields need defaults.
- For durable workflow state, prefer explicit schemas such as dataclasses or `msgspec.Struct` types.
- Steps must be instance methods decorated with `{py:func}``@minion_step``. They run in source order.
- Use `self.event` to access the current pipeline event; the event is contextvar-bound per workflow.
- Use `self.context` to read and update the current workflow context; steps do not receive it as an argument.
- Keep per-workflow mutable state in `self.context`. Regular Minion instance attributes are shared by that Minion's concurrent workflows; see {ref}`component-state-ownership` for the complete state-scope model.
- Use `self.workflow_handle` when business code needs optional diagnostic correlation data for logs or audit records. The read-only handle exposes `orchestration_id` and `workflow_id`, matching the stable identity fields used by framework diagnostics and persisted workflow state.
- Raise `{py:class}``minions._internal._domain.exceptions.AbortWorkflow`` to stop a workflow gracefully without treating it as a failure.
- Do not raise `asyncio.CancelledError` to intentionally stop a workflow. The runtime treats cancellation as an interruption, keeps the persisted workflow context, and may resume the workflow later. Use `AbortWorkflow` when the workflow should stop as an intentional terminal outcome.

### Async work ownership

Await asynchronous work directly so its result, failure, and cancellation remain
owned by the current Minion operation. Minions rejects `asyncio.create_task` and
`asyncio.ensure_future` in user code because those APIs can create detached work
that outlives its workflow or component and bypasses runtime failure reporting
and shutdown. When a component intentionally owns background work, use
`self.safe_create_task(...)` so Minions retains, supervises, and drains it.

Use `await asyncio.to_thread(...)` for short, bounded synchronous I/O that would
otherwise block the event loop. Exceptions raised in the worker thread are
re-raised at the `await` point and follow the current operation's normal failure
path. Do not detach the returned coroutine into a separate task.

Cancelling the awaiting operation does not stop synchronous code that has
already started in the worker thread. Threaded work must therefore be
thread-safe and safe to finish after workflow or component cancellation. Use a
cooperative cancellation mechanism or a separately supervised process for
long-running or non-idempotent work.

### Typed configuration

Use Minion configuration to keep deployment settings out of workflow code while
giving steps a validated, typed model. For production, use file-backed
configuration. Declare the framework-defined `config` attribute with the model
type your steps expect, and override `load_config` to parse the supplied file:

```python
import asyncio
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass
class OrderConfig:
    region: str


class OrderMinion(Minion[OrderEvent, WorkflowCtx]):
    config: OrderConfig

    async def load_config(self, config_path: str) -> OrderConfig:
        raw = await asyncio.to_thread(Path(config_path).read_text)
        values = json.loads(raw)
        return OrderConfig(region=values["region"])

    @minion_step
    async def process_order(self):
        print(f"Processing order in {self.config.region}")
```

During startup, Minions calls `load_config`, verifies that its result is a
dataclass or `msgspec.Struct` instance matching the annotation, and binds it to
`self.config` before any workflow starts. Invalid configuration therefore
prevents the Minion from starting. The public attribute name is always
`config`; another annotated attribute does not participate in configuration
binding.

The stamped file may contain the complete configuration or act as a manifest
for a remote configuration source:

```yaml
_minions_config_id: "44444444-4444-4444-8444-444444444444"
provider: "database"
config_key: "clients/acme/order-processing"
```

An async `load_config` can read that manifest and fetch the referenced values
through a database, secrets service, or HTTP client. It must return a
materialized typed snapshot, not a live client or proxy whose effective values
change during workflow execution. Use `asyncio.to_thread` when the loader performs
blocking filesystem or other synchronous I/O.

Configuration also participates in orchestration identity. Starting the same
Minion and Pipeline with different config identities creates distinct
orchestrations, so one implementation can run independently for different
clients, campaigns, or deployment profiles. For durable file-backed
deployments, give each configuration a unique `_minions_config_id`; otherwise
Gru uses the config path as a fallback identity. Changing a file's contents
while retaining its stamped ID does not define a new orchestration identity;
the new contents—or the newly resolved remote values—apply when that
configuration is next loaded. Minions loads the snapshot once per Minion
startup and binds it before continuing saved unfinished workflows. Changes made
under a stable config ID must therefore remain compatible with those workflows;
use a different config ID when the revision should create a distinct orchestration.
See {doc}`state-and-persistence` for the complete identity and migration
contract.

Configuration is opt-in at orchestration startup. A Minion started without
configuration has no bound `config` attribute; classes that do not use
configuration need not declare `config` or override `load_config`. Inline
configuration is supported for tests and direct local composition; Gru receives
the model by value and binds an independent snapshot, so later mutations to the
caller's object do not affect the Minion. Treat the bound `self.config` snapshot
as immutable; start another orchestration with a new config value when its
settings need to change. The `config` annotation is still required, but a
`load_config` override is not. Use stamped file-backed configuration for durable
production deployments.

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
