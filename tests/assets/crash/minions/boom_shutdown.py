from minions import minion_step

from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext


class BoomShutdownMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "boom-shutdown-minion"

    async def shutdown(self) -> None:
        boom()

    @minion_step
    async def step_1(self) -> None:
        self.context.handled = True


minion = BoomShutdownMinion
