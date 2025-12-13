```{toctree}
:maxdepth: 2
:hidden:

getting-started
how-to/run-your-first-minion
how-to/writing-a-custom-resource
how-to/integrating-with-cli
reference/cli
reference/api/minions_core
reference/api/minions_cli
guides/patterns-and-anti-patterns
guides/testing-minions
guides/deployment-strategies
guides/migrating-to-microservices
guides/scale-out-strategies
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Concepts

concepts/overview
concepts/minions
concepts/pipelines
concepts/resources
concepts/state-and-persistence
concepts/concurrency-and-backpressure
concepts/runtime-modes
concepts/project-structure
concepts/portability
```

```{toctree}
:maxdepth: 2
:hidden:
:caption: Advanced

advanced/sidecar-resources
advanced/minimal-installs
```

```{toctree}
:maxdepth: 1
:hidden:
:caption: kitchen skin

kitchen-skin/need-a-nonnative-library
kitchen-skin/contributing
kitchen-skin/writing-minions
```

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

If you don’t need container-grade isolation or horizontal scaling, Minions is often simpler, faster, and more efficient.
Need more throughput? See {doc}`guides/scale-out-strategies`.(you can move system components out incrementally) If your needs change, Minions components map 1:1 to microservices components—see {doc}`guides/migrating-to-microservices`.
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
## Microservices vs Minions Example

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
from minions import Minion, Pipeline, Resource

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

## Next steps

- Start with {doc}`getting-started` for a concrete “hello minion” walkthrough  
- Read {doc}`concepts/overview` to understand the Minions mental model  
- Jump to {doc}`how-to/run-your-first-minion` for a hands-on checklist  
- Explore {doc}`reference/api/minions_core` as the API surface evolves
