from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.seq = self.event.seq

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = AssetMinion
