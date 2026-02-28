import asyncio

from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.events.counter import CounterEvent


class SlowStepMinion(SpiedMinion[CounterEvent, dict]):
    name = "slow-step-minion"

    @minion_step
    async def step_1(self):
        # Keep workflow in-flight long enough for stop/persistence assertions.
        await asyncio.sleep(2)


minion = SlowStepMinion
