> **Status:** Pre-alpha (`0.0.x`). APIs and docs are still evolving and may change without notice.

<p align="center">
  <img src="docs/_static/mascot-856.png" width="256" alt="Minions mascot">
</p>

# Minions

Get the benefits of microservices without the complexity of running a distributed system.

Minions is a single-process, Python-native runtime that coordinates your system’s long-lived components.

Define your components as Python classes, declare your orchestrations, and the runtime handles everything else—state, lifecycle, and resumability.

## Why Minions instead of microservices?

- One process, one deploy — no containers, queues, gRPC, or distributed ops
- Pure Python development with no distributed debugging or infra burden
- Built-in orchestration, lifecycle, dependency management, metrics, and state
- Safe, dependency-aware restarts and redeployment of individual components
- Fully portable: a Minions system is just a project folder.  
  Copy it anywhere and the entire orchestration — code, configs, and state — runs exactly the same.

If you don’t need container-grade isolation or horizontal scaling, Minions is almost always simpler, faster, and more efficient.  
Minions also lets you isolate risky or unstable code so that failures in those parts never bring down the runtime.

<!-- ## Minions vs Microservices (side-by-side) -->
<!-- 
    TODO:
    I imagine this section to be a list examples that prove
    how much better minions is than microservices in terms of
    how much it costs to operate, how much less time and effort
    it is to operate, etc. start first with reference examples
    and in the future i can be testimonials of users almost
-->
## Microservices vs Minions (on-chain crypto trading bot)

Imagine an on-chain trading bot that:

- Listens to events from a chain over WebSocket
- Pulls on-chain/DEX prices for each new event
- Applies a strategy and, when conditions match, submits a transaction

### A typical microservices setup

A common microservice-style design might look like:

- `event-listener-service` (WebSocket subscriber → pushes events to a queue)
- `price-oracle-service` (HTTP/gRPC API for price data)
- `strategy-executor-service` (consumes events, calls price service, decides trades)
- Shared state in Redis/Postgres for positions / risk limits
- Message broker (Kafka/RabbitMQ/etc.) for event fan-out
- Containers + orchestrator (Docker/Kubernetes/etc.) for each service
- CI/CD + deployment scripts for each service
- Centralized logging/metrics stack to stitch everything together

This works, but it comes with the usual overhead:

- Multiple deployable services to build, ship, and observe
- Cross-service versioning and compatibility issues
- Network boundaries and failure modes between every hop
- Distributed debugging when something goes wrong

### The same system with Minions

With Minions, the same shape of system lives inside a single process:
<!-- TODO: update the snippet to latest Minions api -->
```python
class ChainEvents(Resource):
    async def subscribe(self) -> AsyncIterator[ChainEvent]: ...
    

class PriceFeed(Resource):
    async def get_price(self, pair: Pair) -> Price: ...
    

class OnChainStrategy(Minion):
    def __init__(self, events: ChainEvents, prices: PriceFeed):
        self._events = events
        self._prices = prices

    async def run(self) -> None:
        async for event in self._events.subscribe():
            price = await self._prices.get_price(event.pair)
            if should_trade(event, price):
                await submit_tx(event, price)

run(OnChainStrategy) # The runtime wires resources and handles orchestration
```

You still get:

- Clear separation of concerns (events, prices, strategy)
- Long-lived workers and shared resources
- Structured startup/shutdown and dependency management
- Metrics, state, and lifecycle under a single orchestrator

But you only:

- Deploy one process
- Debug one runtime
- Operate one system

Need more throughput? Start with `docs/guides/scale-out-strategies.md`. If you later outgrow the single-process model, see `docs/guides/migrating-to-microservices.md`.

## Installation

The package name is reserved on PyPI, but the project is **not** ready for general use.

If you still want to experiment at your own risk:

```bash
pip install minions
