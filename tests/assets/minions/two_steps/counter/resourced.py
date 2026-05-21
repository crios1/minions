from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.base import FixedResource


class TwoStepResourcedMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "two-step-resourced-minion"
    fixed_resource: FixedResource

    @minion_step
    async def step_1(self) -> None:
        self.context.value = await self.fixed_resource.get_value(self.event.seq)

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = TwoStepResourcedMinion
