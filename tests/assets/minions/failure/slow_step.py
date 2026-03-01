import asyncio

from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.events.counter import CounterEvent


class SlowStepMinion(SpiedMinion[CounterEvent, dict]):
    name = "slow-step-minion"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        # Instance-level override for this test fixture.
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self):
        # Intentionally "slow" relative to immediate-step fixtures.
        await asyncio.sleep(0.2)


minion = SlowStepMinion
