# Run Your First Minion

A task-focused checklist to launch a minion with Gru.

## 1) Create modules

- `pipelines.py` with a `Pipeline[T_Event]` subclass and module-level `pipeline = YourPipeline`.
- `minions.py` with a `Minion[T_Event, T_Ctx]` subclass and optional `minion = YourMinion`.
- `resources.py` for any shared dependencies declared via type hints on your pipeline/minion.

## 2) Add a config file

Create a TOML/JSON/YAML file even if it is empty today (e.g., `config/print.toml`). Override `load_config` later for validation.

## 3) Start Gru and the minion

```python
import asyncio
from minions import Gru, GruShell

async def main():
    gru = await Gru.create()
    await gru.start_minion(
        "my_app.minions",   # module containing your Minion subclass or `minion`
        "config/print.toml",
        "my_app.pipelines", # module containing your Pipeline subclass or `pipeline`
    )

    shell = GruShell(gru)
    try:
        await asyncio.to_thread(shell.cmdloop)
    finally:
        await gru.shutdown()

asyncio.run(main())
```

## 4) Use GruShell commands

Inside the shell:

- `start MINION_MODULEPATH CONFIG_PATH PIPELINE_MODULEPATH` — start additional minions.
- `stop NAME_OR_INSTANCE_ID ...` — stop by instance ID or minion name.
- `status [--await] [--timeout N] [ids...]` — snapshot or wait for orchestration to settle.
- `shutdown` — gracefully stop everything.

See {doc}`integrating-with-cli` for command details and caveats.

## 5) Observe

- Metrics are exposed via the configured `Metrics` backend (Prometheus by default on port 8081).
- Logs default to file-based; inject your own logger to ship elsewhere.
- Check the state store (`minions.db` for SQLite) to inspect persisted workflow contexts.
