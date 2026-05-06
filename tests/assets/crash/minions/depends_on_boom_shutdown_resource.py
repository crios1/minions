from minions import minion_step

from tests.assets.crash.resources.boom_shutdown import BoomShutdownResource
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class DependsOnBoomShutdownResourceMinion(SpiedMinion[CounterEvent, dict]):
    name = "depends-on-boom-shutdown-resource-minion"
    boom_resource: BoomShutdownResource

    @minion_step
    async def step_1(self):
        self.context["handled"] = True


minion = DependsOnBoomShutdownResourceMinion
