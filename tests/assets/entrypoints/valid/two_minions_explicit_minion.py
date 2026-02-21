from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.event_counter import CounterEvent


class AlphaMinion(SpiedMinion[CounterEvent, dict]):
    name = "alpha-minion"

    @minion_step
    async def step_1(self):
        self.context["alpha"] = True

class BetaMinion(SpiedMinion[CounterEvent, dict]):
    name = "beta-minion"

    @minion_step
    async def step_1(self):
        self.context["beta"] = True


minion = AlphaMinion
