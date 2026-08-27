import asyncio

from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    async def startup(self) -> None:
        self._release_run_failure = asyncio.Event()

    def trigger_run_failure(self) -> None:
        self._release_run_failure.set()

    async def run(self) -> None:
        await self._release_run_failure.wait()
        boom()

    @minion_step
    async def step_1(self) -> None:
        self.context.seq = self.event.seq


minion = AssetMinion
