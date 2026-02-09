from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from .event_counter import CounterEvent
from .resource_fixed import FixedResource


class TwoStepResourcedSharedMinionB(SpiedMinion[CounterEvent, dict]):
    name = "two-step-resourced-shared-minion-b"
    fixed_resource: FixedResource

    @minion_step
    async def step_1(self):
        self.context["value"] = await self.fixed_resource.get_value(self.event.seq)

    @minion_step
    async def step_2(self):
        self.context["handled"] = True


minion = TwoStepResourcedSharedMinionB
