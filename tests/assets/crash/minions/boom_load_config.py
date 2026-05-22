from minions import minion_step

from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext


class BoomLoadConfigMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "boom-load-config-minion"

    async def load_config(self, config_path: str):
        boom()

    @minion_step
    async def step_1(self) -> None:
        self.context.handled = True


minion = BoomLoadConfigMinion
