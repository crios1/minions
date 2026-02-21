from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.event_counter import CounterEvent


class TwoStepMinion(SpiedMinion[CounterEvent, dict]):
    name = "two-step-minion"

    @minion_step
    async def step_1(self):
        self.context["seq"] = self.event.seq

    @minion_step
    async def step_2(self):
        self.context["handled"] = True


minion = TwoStepMinion
