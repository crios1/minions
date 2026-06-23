from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.default import AssetResource as FixedResource
from tests.assets.resources.fixed.default_b import AssetResource as FixedResourceB
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    fixed_resource: FixedResource
    fixed_resource_b: FixedResourceB

    @minion_step
    async def step_1(self) -> None:
        self.context.value_a = await self.fixed_resource.get_value()
        self.context.value_b = await self.fixed_resource_b.get_value()


minion = AssetMinion
