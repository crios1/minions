from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.default_b import AssetResource as SimpleResourceB
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[SimpleEvent, SimpleContext]):
    simple_resource: SimpleResourceB

    @minion_step
    async def step_1(self) -> None:
        self.context.value = await self.simple_resource.get_value()
        self.context.step1 = "step1"

    @minion_step
    async def step_2(self) -> None:
        self.context.step2 = "step2"


minion = AssetMinion
