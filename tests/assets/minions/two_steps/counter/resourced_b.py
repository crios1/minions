from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.base_b import FixedResourceB


class TwoStepResourcedMinionB(SpiedMinion[CounterEvent, CounterContext]):
    name = "two-step-resourced-minion-b"
    fixed_resource_b: FixedResourceB

    @minion_step
    async def step_1(self) -> None:
        self.context.value_b = await self.fixed_resource_b.get_value(self.event.seq)

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = TwoStepResourcedMinionB
