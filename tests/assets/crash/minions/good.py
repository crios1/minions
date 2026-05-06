from minions import minion_step

from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class GoodMinion(SpiedMinion[CounterEvent, dict]):
    name = "crash-good-minion"

    @minion_step
    async def step_1(self):
        self.context["seq"] = self.event.seq


minion = GoodMinion

