from minions import minion_step

from tests.assets.crash.resources.boom_startup import BoomStartupResource
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext


class DependsOnBoomStartupResourceMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "depends-on-boom-startup-resource-minion"
    boom_resource: BoomStartupResource

    @minion_step
    async def step_1(self) -> None:
        self.context.handled = True


minion = DependsOnBoomStartupResourceMinion
