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

## Dependency graph

Resources can depend on other resources via type hints. Gru traverses the graph, starts everything once, reference-counts usage, and shuts down unused nodes when minions or pipelines stop.

```python
class CachedPriceAPI(Resource):
    backend: PriceAPI  # started first, injected automatically
```

The same resource instance is shared wherever the type appears, so rate limiting, connection pools, and caches live in one place.

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

class NewEventPipeline(Pipeline):
    ws: WSClient

    async def run(self):
        async for event in self.ws.conn.iter_events("ticks"):
            yield event  # pipelines emit; they don’t enrich

class PriceWatcher(Minion):
    prices: PriceOracle

    @minion_step
    async def on_event(self, event):
        price = await self.prices.get_price(event.pair)
        ...
```

Gru starts `WSClient` once, injects it into the pipeline (to emit events) and the price oracle (to fetch prices), and tears it down when neither needs it. The minion depends on `PriceOracle`, which depends on `WSClient`, so you get one shared connection across the whole flow.
