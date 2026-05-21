from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.base_c import FixedResourceC


class TwoStepResourcedMinionC(SpiedMinion[CounterEvent, CounterContext]):
    name = "two-step-resourced-minion-c"
    fixed_resource_c: FixedResourceC

    @minion_step
    async def step_1(self) -> None:
        self.context.value_c = await self.fixed_resource_c.get_value(self.event.seq)

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = TwoStepResourcedMinionC
