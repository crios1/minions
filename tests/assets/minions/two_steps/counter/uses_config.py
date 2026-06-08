from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied_configed import ConfiguredSpiedMinion


class ConfigMinion(ConfiguredSpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.config_name = self.config.name

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = ConfigMinion
