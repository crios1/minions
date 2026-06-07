from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.resource_3 import SimpleResource3
from tests.assets.support.minion_spied import SpiedMinion


class SimpleResourcedMinion3(SpiedMinion[SimpleEvent, SimpleContext]):
    simple_resource: SimpleResource3

    @minion_step
    async def step_1(self) -> None:
        print(self.event)
        self.context.price = await self.simple_resource.get_price()
        self.context.step1 = "step1"

    @minion_step
    async def step_2(self) -> None:
        self.context.step2 = "step2"
        print(self.context)


minion = SimpleResourcedMinion3
