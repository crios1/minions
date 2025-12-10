# Recommended Project Structure

Minions systems stay simple when the project layout is simple. The runtime expects only one rule: module paths must be stable. Beyond that, Minions is flexible—but this structure gives you the smoothest experience.

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
Configuration files for deployable minions. Configs are optional but recommended to separate logic from parameters, deploy multiple instances of the same minion, and keep systems portable (config paths stay relative to the project root). Minions stores relative config paths in the composite key, so copying the project preserves identities.

**minions/**  
Your long-lived workers. Each file typically exports one Minion subclass, e.g.:

```python
class TradeExecutor(Minion[TradeEvent, TradeCtx]):
    ...
```

Minion files should be importable as modules like `minions.trade_executor`; this module path is what Minions records.

**pipelines/**  
Event-producing components. Each pipeline defines the “input stream” for a minion:

```python
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
    await gru.start_minion(
        "minions.onchain_strategy",
        "pipelines.price_pipeline",
        minion_config_path="configs/strategy-a.yaml",
    )

if __name__ == "__main__":
    asyncio.run(main())
```

This is where operators add, remove, or redeploy components.

## Why this structure works well

1. **Clean module paths → clean deploys**: Minions identifies components by module path; a simple layout keeps identities predictable.  
2. **Portability is built in**: configs, modpaths, and state live inside the project, so you can copy the folder and run anywhere.  
3. **Separation of concerns without microservice complexity**: minions = long-lived workers; pipelines = ingress; resources = shared infra; orchestrations = composition.  
4. **Fast onboarding**: new contributors can quickly see where workflows live, where events originate, what’s shared, and how the system is launched.

## Variations

Minions isn’t strict. You can flatten or split by domain as needed:

- Tiny projects: keep minions and pipelines in one folder.  
- Larger systems: split by domain (`trading/`, `marketdata/`).  
- Multiple entrypoints: separate orchestrations for `prod/`, `staging/`, etc.  

As long as module paths remain stable, the runtime will work.
