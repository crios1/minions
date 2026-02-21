from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.base import FixedResource
from tests.assets.resources.fixed.base_b import FixedResourceB


class TwoResourceMinion(SpiedMinion[CounterEvent, dict]):
    name = "two-resource-minion"
    fixed_resource: FixedResource
    fixed_resource_b: FixedResourceB

    @minion_step
    async def step_1(self):
        self.context["value_a"] = await self.fixed_resource.get_value(self.event.seq)
        self.context["value_b"] = await self.fixed_resource_b.get_value(self.event.seq)


minion = TwoResourceMinion
