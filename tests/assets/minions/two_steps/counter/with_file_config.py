from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied_configed import ConfiguredSpiedMinion


class AssetMinion(ConfiguredSpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        return

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = AssetMinion
