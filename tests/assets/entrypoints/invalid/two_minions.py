from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.event_counter import CounterEvent


class FirstMinion(SpiedMinion[CounterEvent, dict]):
    name = "first-minion"

    @minion_step
    async def step_1(self):
        self.context["first"] = True

class SecondMinion(SpiedMinion[CounterEvent, dict]):
    name = "second-minion"

    @minion_step
    async def step_1(self):
        self.context["second"] = True
