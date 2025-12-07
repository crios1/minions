# Resources

Resources are shared async services used by pipelines and minions. They encapsulate expensive or stateful dependencies (HTTP clients, DB pools, rate limiters) and provide tracking out of the box.

## Lifecycle and tracking

- Implement `startup`, `run`, and `shutdown` as needed.
- Public async methods are automatically wrapped to record latency and errors via Prometheus metrics and structured logs.
- Use `{py:func}``Resource.untracked`` when you need a method to skip tracking.

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

Resources can depend on other resources by type hinting attributes. Gru traverses the graph, starts everything once, reference-counts usage, and shuts down unused resources when minions or pipelines stop.

```python
class CachedPriceAPI(Resource):
    backend: PriceAPI  # started first, injected automatically
```

When Gru shuts down a minion, it decrements resource reference counts and stops resources that are no longer in use.
