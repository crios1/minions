from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.default import AssetResource as SimpleResource
from tests.assets.support.minion_spied_configed import ConfiguredSpiedMinion


class AssetMinion(ConfiguredSpiedMinion[SimpleEvent, SimpleContext]):
    simple_resource: SimpleResource

    @minion_step
    async def step_1(self) -> None:
        if self.context.value:
            return
        self.context.value = await self.simple_resource.get_value()

    @minion_step
    async def step_2(self) -> None:
        return


minion = AssetMinion
