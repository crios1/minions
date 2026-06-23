from dataclasses import dataclass

from minions import minion_step
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


@dataclass
class AlphaBetaContext:
    alpha: bool = False
    beta: bool = False


class AlphaMinion(SpiedMinion[CounterEvent, AlphaBetaContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.alpha = True


class BetaMinion(SpiedMinion[CounterEvent, AlphaBetaContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.beta = True


minion = AlphaMinion
