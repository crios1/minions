from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets_revamp.support.event_counter import CounterEvent


class ConfigMinion(SpiedMinion[CounterEvent, dict]):
    name = "config-minion"

    @minion_step
    async def step_1(self):
        name = None
        if self._mn_config:
            name = self._mn_config.get("config", {}).get("name")
        self.context["config_name"] = name

    @minion_step
    async def step_2(self):
        self.context["handled"] = True


minion = ConfigMinion
