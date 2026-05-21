from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent


class FirstMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "first-minion"

    @minion_step
    async def step_1(self) -> None:
        self.context.alpha = True

class SecondMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "second-minion"

    @minion_step
    async def step_1(self) -> None:
        self.context.beta = True
