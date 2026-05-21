from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent


class TwoStepMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "two-step-minion"

    @minion_step
    async def step_1(self) -> None:
        self.context.seq = self.event.seq

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = TwoStepMinion
