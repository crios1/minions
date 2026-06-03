# Getting Started

A quick “hello minion” to see how the pieces fit together. Everything here runs in a single Python process.

## Install the docs + dev stack

```bash
python -m pip install -e .[docs]
```

The library itself is still pre-alpha; expect churn while APIs settle.

## Define your domain types

Pick structured event and workflow context types. Minions supports dataclasses
and `msgspec.Struct` models for these domain boundaries.
Use immutable event types when possible (for example, frozen dataclasses) and keep mutable state in `WorkflowCtx`.

```python
# my_app/types.py
from dataclasses import dataclass

@dataclass
class Heartbeat:
    timestamp: float

@dataclass
class WorkflowCtx:
    user_id: str
    retries: int = 0
```

## Create a Resource

Resources are shared dependencies with startup/shutdown hooks. Public async methods are automatically latency/error-tracked unless you mark them `{py:func}``Resource.untracked``.

```python
# my_app/resources.py
from minions import Resource

class HeartbeatStore(Resource):
    async def startup(self):
        self.seen = []

    async def save(self, heartbeat: Heartbeat):
        self.seen.append(heartbeat)
```

## Build a Pipeline

Pipelines emit events forever. The framework fans them out to subscribed minions.

```python
# my_app/pipelines.py
import asyncio, time
from minions import Pipeline
from .types import Heartbeat

class HeartbeatPipeline(Pipeline[Heartbeat]):
    async def produce_event(self) -> Heartbeat:
        await asyncio.sleep(1)
        return Heartbeat(timestamp=time.time())

pipeline = HeartbeatPipeline  # helps Gru resolve the class
```

## Write a Minion

Minions declare the event and workflow context types, then implement ordered `{py:func}``@minion_step`` methods.

```python
# my_app/minions.py
from minions import Minion, minion_step
from .resources import HeartbeatStore
from .types import Heartbeat, WorkflowCtx

class PrintMinion(Minion[Heartbeat, WorkflowCtx]):
    store: HeartbeatStore  # dependency injected automatically
    name = "print-minion"

    @minion_step
    async def record(self, ctx: WorkflowCtx):
        await self.store.save(self.event)
        ctx.retries += 1
        print(f"[{ctx.user_id}] heartbeat at {self.event.timestamp}")

minion = PrintMinion  # optional, makes discovery unambiguous
```

## Run everything with Gru

`Gru` orchestrates lifecycles: starting pipelines/resources, wiring dependencies, subscribing minions, and persisting workflow context. The current API accepts module paths for your pipeline/minion classes. Pass `minion_config_path` only when that Minion declares a typed `config` attribute, overrides `load_config`, and returns a dataclass or `msgspec.Struct` config model.

```python
# run.py
import asyncio
from minions import Gru, GruShell

async def main():
    gru = await Gru.create()

    await gru.start_orchestration(
        "my_app.minions",   # module containing one local Minion subclass
        "my_app.pipelines", # module containing one local Pipeline subclass
    )

    shell = GruShell(gru)
    try:
        await asyncio.to_thread(shell.cmdloop)  # deprecated local helper
    finally:
        await gru.shutdown()

if __name__ == "__main__":
    asyncio.run(main())
```

Start the app and try `start`, `stop`, `status`, and `shutdown` in the shell. `GruShell` is a deprecated transitional helper; the planned long-term runtime control model is `minions gru serve` plus `minions gru attach`. See {doc}`how-to/integrating-with-cli` for current command details.

## Next steps

- Read {doc}`concepts/overview` to internalize the mental model.
- Use {doc}`how-to/writing-a-custom-resource` to wire real dependencies.
- Check {doc}`guides/patterns-and-anti-patterns` for design guardrails.
