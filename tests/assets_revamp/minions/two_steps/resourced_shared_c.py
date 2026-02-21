from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets_revamp.support.event_counter import CounterEvent
from tests.assets_revamp.resources.fixed.base import FixedResource


class TwoStepResourcedSharedMinionC(SpiedMinion[CounterEvent, dict]):
    name = "two-step-resourced-shared-minion-c"
    fixed_resource: FixedResource

    @minion_step
    async def step_1(self):
        self.context["value"] = await self.fixed_resource.get_value(self.event.seq)

    @minion_step
    async def step_2(self):
        self.context["handled"] = True


minion = TwoStepResourcedSharedMinionC
