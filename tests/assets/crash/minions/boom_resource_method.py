from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.crash.resources.boom_method import BoomMethodResource
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class BoomResourceMethodMinion(SpiedMinion[CounterEvent, CounterContext]):
    boom_resource: BoomMethodResource

    @minion_step
    async def step_1(self) -> None:
        await self.boom_resource.explode()


minion = BoomResourceMethodMinion
