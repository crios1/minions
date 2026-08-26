import asyncio

from minions import minion_id, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


@minion_id("52345678-1234-5678-9234-567812345678")
class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        # Intentionally "slow" relative to immediate-step fixtures.
        await asyncio.sleep(0.2)


minion = AssetMinion
