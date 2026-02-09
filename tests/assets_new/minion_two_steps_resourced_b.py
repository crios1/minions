from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from .event_counter import CounterEvent
from .resource_fixed_b import FixedResourceB


class TwoStepResourcedMinionB(SpiedMinion[CounterEvent, dict]):
    name = "two-step-resourced-minion-b"
    fixed_resource_b: FixedResourceB

    @minion_step
    async def step_1(self):
        self.context["value_b"] = await self.fixed_resource_b.get_value(self.event.seq)

    @minion_step
    async def step_2(self):
        self.context["handled"] = True


minion = TwoStepResourcedMinionB
