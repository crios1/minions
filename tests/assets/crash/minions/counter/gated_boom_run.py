import asyncio

from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    release_run_failure = asyncio.Event()

    async def run(self) -> None:
        await type(self).release_run_failure.wait()
        boom()

    @minion_step
    async def step_1(self) -> None:
        self.context.seq = self.event.seq


minion = AssetMinion
