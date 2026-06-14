import asyncio
from typing import Any

from minions import minion_id, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion

SLOW_STEP_MINION_ID = "52345678-1234-5678-9234-567812345678"


@minion_id(SLOW_STEP_MINION_ID)
class SlowStepMinion(SpiedMinion[CounterEvent, CounterContext]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        # Instance-level override for this test fixture.
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self) -> None:
        # Intentionally "slow" relative to immediate-step fixtures.
        await asyncio.sleep(0.2)


minion = SlowStepMinion
