from minions import minion_step
from minions._internal._domain.exceptions import AbortWorkflow
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    @minion_step
    async def step_1(self) -> None:
        raise AbortWorkflow("intentional abort")


minion = AssetMinion
