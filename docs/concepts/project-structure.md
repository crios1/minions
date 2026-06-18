# Recommended Project Structure

Minions systems stay simple when the project layout is simple. Id-less components use module paths as fallback identity, so stable module paths are enough while prototyping. For durable systems, stamp component UUIDs so minions, pipelines, and resources keep the same identity across normal file moves and renames.

```
my-project/
│
├── configs/
│   ├── strategy-a.yaml
│   └── strategy-b.yaml
│
├── minions/
│   ├── price_feed.py
│   ├── chain_events.py
│   └── onchain_strategy.py
│
├── pipelines/
│   ├── price_pipeline.py
│   └── event_pipeline.py
│
├── resources/
│   └── http_client.py
│
├── state/
│   └── minions.sqlite3        # runtime persistence (default location)
│
├── orchestrations/
│   └── launch.py              # declares which minions to start
│
└── pyproject.toml
```

## What each folder is for

**configs/**  
Configuration files for deployable minions. Configs are optional but recommended to separate logic from parameters, deploy multiple instances of the same minion, and keep systems portable. A Minion that uses file config declares a typed `config` attribute, overrides `load_config`, and returns a dataclass or `msgspec.Struct` config model. TOML, YAML, and JSON config files can carry a top-level `_minions_config_id` UUID so file moves and renames preserve the config identity. Config files without `_minions_config_id` still use path fallback identity. Gru derives the public `orchestration_id` as an opaque deterministic SHA-256 ID from the minion, pipeline, and config identities.

**minions/**  
Your long-lived workers. Each file typically exports one Minion subclass, e.g.:

```python
from minions import minion_id

@minion_id("11111111-1111-4111-8111-111111111111")
class TradeExecutor(Minion[TradeEvent, TradeCtx]):
    ...
```

Minion files should be importable as modules like `minions.trade_executor`. If the class has `@minion_id(...)`, Gru uses that UUID as the durable component identity; otherwise it falls back to the module path for prototype/non-durable usage.

**pipelines/**  
Event-producing components. Each pipeline defines the “input stream” for a minion:

```python
from minions import pipeline_id

@pipeline_id("22222222-2222-4222-8222-222222222222")
class PricePipeline(Pipeline[PriceEvent]):
    ...
```

Keeping pipelines separate mirrors how real systems have distinct ingress points.

**resources/**  
Reusable shared utilities (DB clients, HTTP clients, RPC connections, etc.). Resources are loaded once and shared across minions. Keeping them together keeps imports predictable and avoids circular-import traps.

**state/**  
Where Minions stores runtime persistence. Default: `state/minions.sqlite3`. Everything stored here is portable; a deployment can be paused, zipped, moved, and resumed unchanged.

**orchestrations/**  
Entry points that start your system:

```python
async def main():
    gru = await Gru.create()
    await gru.start_orchestration(
        "pipelines.price_pipeline",
        "minions.onchain_strategy",
        minion_config_path="configs/strategy-a.yaml",
    )

if __name__ == "__main__":
    asyncio.run(main())
```

This is where operators add, remove, or redeploy components.

## Why this structure works well

1. **Explicit component UUIDs → refactor-stable deploys**: stamped component IDs move with the source class, while id-less components keep the low-friction module-path fallback for exploration.  
2. **Portability is built in**: configs, module paths, component IDs, and state live inside the project, so you can copy the folder and run anywhere.  
3. **Separation of concerns without microservice complexity**: minions = long-lived workers; pipelines = ingress; resources = shared infra; orchestrations = composition.  
4. **Fast onboarding**: new contributors can quickly see where workflows live, where events originate, what’s shared, and how the system is launched.

## Variations

Minions isn’t strict. You can flatten or split by domain as needed:

- Tiny projects: keep minions and pipelines in one folder.  
- Larger systems: split by domain (`trading/`, `marketdata/`).  
- Multiple entrypoints: separate orchestrations for `prod/`, `staging/`, etc.  

As long as module paths remain importable, the runtime will work. For durable refactor-stable identity, stamp component IDs before relying on persisted workflow resume or metric continuity across source moves.
