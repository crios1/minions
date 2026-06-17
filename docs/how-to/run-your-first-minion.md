# Run Your First Minion

A task-focused checklist to launch a minion with Gru.

## 1) Create modules

- `pipelines.py` with one `Pipeline[T_Event]` subclass.
- `minions.py` with one `Minion[T_Event, T_Ctx]` subclass.
- `resources.py` for any shared dependencies declared via type hints on your pipeline/minion.

## 2) Add a config model and loader

File-backed config is optional. When a minion uses it, override `load_config`
and return a dataclass or `msgspec.Struct` model so invalid config fails before
workflows start.

```python
from dataclasses import dataclass
import json
from pathlib import Path


@dataclass
class PrintConfig:
    prefix: str


class PrintMinion(Minion[PrintEvent, PrintContext]):
    config: PrintConfig

    async def load_config(self, config_path: str) -> PrintConfig:
        raw = json.loads(Path(config_path).read_text())
        return PrintConfig(prefix=raw["prefix"])
```

## 3) Start Gru and the minion

```python
import asyncio
from minions import Gru, GruShell

async def main():
    gru = await Gru.create()
    await gru.start_orchestration(
        "my_app.minions",   # module containing one local Minion subclass
        "my_app.pipelines", # module containing one local Pipeline subclass
        minion_config_path="config/print.json",
    )

    shell = GruShell(gru)
    try:
        await asyncio.to_thread(shell.cmdloop)
    finally:
        await gru.shutdown()

asyncio.run(main())
```

`GruShell` is a deprecated transitional helper: it starts an interactive control loop in the same process, and exiting it tears down the local runtime. It remains useful for local experimentation until the planned `minions gru serve` / `minions gru attach` flow replaces it.

## 4) Use GruShell commands

Inside the shell:

- `start MINION_MODULE_PATH MINION_CONFIG_PATH PIPELINE_MODULE_PATH` — start additional orchestrations.
- `stop ORCHESTRATION_ID ...` — stop by orchestration ID.
- `status [--await] [--timeout N] [ids...]` — snapshot or wait for orchestration to settle.
- `shutdown` — gracefully stop everything.

See {doc}`integrating-with-cli` for command details and caveats.

## 5) Observe

- Metrics are exposed via the configured `Metrics` backend (Prometheus by default on port 8081).
- Logs default to file-based; inject your own logger to ship elsewhere.
- Check the state store (`minions.db` for SQLite) to inspect persisted workflow contexts.
- See {doc}`/guides/operating-with-metrics` for how to interpret persistence and workflow telemetry once the runtime is live.
