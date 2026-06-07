from minions import minion_id, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.identified import IdentifiedFixedResource
from tests.assets.support.minion_spied import SpiedMinion

IDENTIFIED_COUNTER_MINION_ID = "32345678-1234-5678-9234-567812345678"


@minion_id(IDENTIFIED_COUNTER_MINION_ID)
class IdentifiedResourcedMinion(SpiedMinion[CounterEvent, CounterContext]):
    fixed_resource: IdentifiedFixedResource

    @minion_step
    async def step_1(self) -> None:
        self.context.value = await self.fixed_resource.get_value(self.event.seq)

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = IdentifiedResourcedMinion
