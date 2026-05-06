from minions import minion_step

from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class BoomLoadConfigMinion(SpiedMinion[CounterEvent, dict]):
    name = "boom-load-config-minion"

    async def load_config(self, config_path: str) -> dict: # pyright: ignore[reportReturnType]
        boom()

    @minion_step
    async def step_1(self):
        self.context["handled"] = True


minion = BoomLoadConfigMinion

