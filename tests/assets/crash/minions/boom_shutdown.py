from minions import minion_step

from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class BoomShutdownMinion(SpiedMinion[CounterEvent, dict]):
    name = "boom-shutdown-minion"

    async def shutdown(self) -> None:
        boom()

    @minion_step
    async def step_1(self):
        self.context["handled"] = True


minion = BoomShutdownMinion

