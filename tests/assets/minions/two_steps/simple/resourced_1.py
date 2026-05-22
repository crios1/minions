from minions import minion_step

from tests.assets.support.minion_spied_configed import ConfiguredSpiedMinion
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.resource_1 import SimpleResource1


class SimpleResourcedMinion1(ConfiguredSpiedMinion[SimpleEvent, SimpleContext]):
    name = "simple-resourced-minion-1"
    simple_resource: SimpleResource1

    @minion_step
    async def step_1(self) -> None:
        if self.context.price:
            return
        self.context.price = await self.simple_resource.get_price(self.event.timestamp)

    @minion_step
    async def step_2(self) -> None:
        print(self.context)


minion = SimpleResourcedMinion1
