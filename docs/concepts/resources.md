# Resources

Resources are shared async services used by pipelines and minions. They encapsulate expensive or stateful dependencies (HTTP clients, DB pools, rate limiters) and provide tracking out of the box.

## Lifecycle and tracking

- Implement `startup`, `run`, and `shutdown` as needed.
- Public async methods are automatically wrapped to record latency and errors via Prometheus metrics and structured logs.
- Use `{py:func}``Resource.untracked`` when you need a method to skip tracking (e.g., health checks).

```python
from minions import Resource

class PriceAPI(Resource):
    async def startup(self):
        self.client = ...

    async def get_price(self, symbol: str) -> float:
        return await self.client.fetch(symbol)

    @Resource.untracked
    async def healthcheck(self) -> bool:
        return True
```

## Failure logs and argument privacy

When a tracked Resource method fails, Minions increments the counter metric
named `resource_error_total` and emits a structured `Resource method failed`
log. The log includes Resource identity, method name, error type, exception
details, and safe invocation metadata: argument count, parameter names, and
runtime type names.

Invocation metadata does not include the values passed through the method's
`args` or `kwargs`, so Minions can identify the failing operation without
recording those values. This protection applies only to metadata Minions adds:
exception messages, chained-cause messages, tracebacks, and custom exception
context are recorded as supplied. Do not put secrets in those fields; use
identifiers or redacted summaries when raising or enriching exceptions.
Applications with stricter requirements should enforce them in their exception
and logger implementations.

## Dependency graph

Resources can depend on other resources via type hints. Gru traverses the graph, starts everything once, reference-counts usage, and shuts down unused nodes when minions or pipelines stop.
See {ref}`runtime-component-sharing` for how this fits into the broader orchestration sharing model.

```python
class CachedPriceAPI(Resource):
    backend: PriceAPI  # started first, injected automatically
```

The same resource instance is shared wherever the type appears, so rate limiting, connection pools, and caches live in one place. Initialize that mutable shared state on `self`, normally in `startup`, so it belongs to the running Resource lifecycle and resets if Gru later creates a replacement instance. Do not use mutable class attributes as Resource storage; see {ref}`component-state-ownership`.

### Example: shared connection reused by a minion and its pipeline

Pipelines emit events; minions enrich/act on them. A resource-of-a-resource lets both reuse the same underlying client.

```python
from minions import Resource, Pipeline, Minion, minion_step

class WSClient(Resource):
    async def startup(self):
        self.conn = await connect_ws(...)

class PriceOracle(Resource):
    ws: WSClient  # started first, injected automatically

    async def get_price(self, pair: str) -> float:
        await self.ws.conn.send_json({"pair": pair})
        return await read_price(self.ws.conn)

class NewEventPipeline(Pipeline[TickEvent]):
    ws: WSClient

    async def produce_event(self) -> TickEvent:
        return await self.ws.conn.next_event("ticks")

class PriceWatcher(Minion):
    prices: PriceOracle

    @minion_step
    async def on_event(self, event):
        price = await self.prices.get_price(event.pair)
        ...
```

Gru starts `WSClient` once, injects it into the pipeline (to emit events) and the price oracle (to fetch prices), and tears it down when neither needs it. The minion depends on `PriceOracle`, which depends on `WSClient`, so you get one shared connection across the whole flow.
