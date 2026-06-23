from minions import minion_id, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.identified import AssetResource as IdentifiedFixedResource
from tests.assets.support.minion_spied import SpiedMinion


@minion_id("32345678-1234-5678-9234-567812345678")
class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    fixed_resource: IdentifiedFixedResource

    @minion_step
    async def step_1(self) -> None:
        self.context.value = await self.fixed_resource.get_value(self.event.seq)

    @minion_step
    async def step_2(self) -> None:
        self.context.handled = True


minion = AssetMinion
