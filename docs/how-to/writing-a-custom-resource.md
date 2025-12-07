# Writing a Custom Resource

Resources package shared dependencies and give you lifecycle + metrics for free. Here’s how to create one safely.

## 1) Define the class

```python
from minions import Resource

class PriceAPI(Resource):
    async def startup(self):
        self.client = ...  # e.g., httpx.AsyncClient

    async def shutdown(self):
        await self.client.aclose()

    async def get_price(self, symbol: str) -> float:
        return await self.client.get_price(symbol)
```

Public async methods are wrapped automatically:

- Latency recorded in `RESOURCE_LATENCY_SECONDS`.
- Errors logged with context and counted in `RESOURCE_ERROR_TOTAL`.
- Successful calls counted in `RESOURCE_SERVES_TOTAL`.

Mark methods that should skip tracking with `{py:func}``Resource.untracked``.

## 2) Declare dependencies

Type-hint other resources to express dependencies; Gru will start them first and inject instances.

```python
class CachedAPI(Resource):
    backend: PriceAPI
```

## 3) Use from minions or pipelines

Add a type-hinted attribute and call methods directly.

```python
class PriceMinion(Minion[PriceEvent, Ctx]):
    api: PriceAPI

    @minion_step
    async def fetch(self, ctx: Ctx):
        ctx.quote = await self.api.get_price(ctx.symbol)
```

## 4) Rate limit where it matters

Implement semaphores/backoff inside the resource to provide backpressure without slowing unrelated workflows.

## 5) Test in isolation

- Instantiate the resource with test doubles for logger/metrics.
- Call public methods directly; they will still be wrapped for tracking, so assert metrics/log behavior.
- Consider using no-op or in-memory metrics/logger helpers from `tests/assets/support` as inspiration.
