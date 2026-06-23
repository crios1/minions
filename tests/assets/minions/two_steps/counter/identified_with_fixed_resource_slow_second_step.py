import asyncio
from typing import Any

from minions import minion_id, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.identified import AssetResource as IdentifiedFixedResource
from tests.assets.support.minion_spied import SpiedMinion


@minion_id("42345678-1234-5678-9234-567812345678")
class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    fixed_resource: IdentifiedFixedResource

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self) -> None:
        self.context.value = await self.fixed_resource.get_value()

    @minion_step
    async def step_2(self) -> None:
        await asyncio.sleep(0.2)
        self.context.handled = True


minion = AssetMinion
