from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class AlphaMinion(SpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.alpha = True


class BetaMinion(SpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.beta = True


minion = AlphaMinion
