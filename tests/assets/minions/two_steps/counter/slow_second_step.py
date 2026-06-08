import asyncio
from typing import Any

from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class SlowSecondStepMinion(SpiedMinion[CounterEvent, CounterContext]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Keep stop-driven checkpoint tests fast while still giving the stop call
        # a deterministic in-flight second step to interrupt.
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self) -> None:
        self.context.seq = self.event.seq

    @minion_step
    async def step_2(self) -> None:
        await asyncio.sleep(0.2)
        self.context.handled = True


minion = SlowSecondStepMinion
