from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.default_b import AssetResource as FixedResourceB
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    fixed_resource_b: FixedResourceB

    @minion_step
    async def step_1(self) -> None:
        self.context.value_b = await self.fixed_resource_b.get_value()

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = AssetMinion
