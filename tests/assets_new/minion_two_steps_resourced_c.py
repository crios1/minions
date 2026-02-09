from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from .event_counter import CounterEvent
from .resource_fixed_c import FixedResourceC


class TwoStepResourcedMinionC(SpiedMinion[CounterEvent, dict]):
    name = "two-step-resourced-minion-c"
    fixed_resource_c: FixedResourceC

    @minion_step
    async def step_1(self):
        self.context["value_c"] = await self.fixed_resource_c.get_value(self.event.seq)

    @minion_step
    async def step_2(self):
        self.context["handled"] = True


minion = TwoStepResourcedMinionC
