from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.resource_2 import SimpleResource2
from tests.assets.support.minion_spied import SpiedMinion


class SimpleResourcedMinion2(SpiedMinion[SimpleEvent, SimpleContext]):
    simple_resource: SimpleResource2

    @minion_step
    async def step_1(self) -> None:
        print(self.event)
        self.context.price = await self.simple_resource.get_price()
        self.context.step1 = "step1"

    @minion_step
    async def step_2(self) -> None:
        self.context.step2 = "step2"
        print(self.context)


minion = SimpleResourcedMinion2
