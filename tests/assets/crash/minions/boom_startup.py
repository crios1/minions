from minions import minion_step

from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class BoomStartupMinion(SpiedMinion[CounterEvent, dict]):
    name = "boom-startup-minion"

    async def startup(self) -> None:
        boom()

    @minion_step
    async def step_1(self):
        self.context["handled"] = True


minion = BoomStartupMinion

