from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent


class ConfigMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "config-minion"

    @minion_step
    async def step_1(self) -> None:
        name = None
        if self._mn_config:
            name = self._mn_config.get("config", {}).get("name")
        self.context.config_name = name

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = ConfigMinion
