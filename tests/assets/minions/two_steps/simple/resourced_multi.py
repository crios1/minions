from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.resource_1 import SimpleResource1
from tests.assets.resources.simple.resource_2 import SimpleResource2
from tests.assets.support.minion_spied import SpiedMinion


class SimpleMultiResourcedMinion(SpiedMinion[SimpleEvent, SimpleContext]):
    r1: SimpleResource1
    r2: SimpleResource2

    @minion_step
    async def step_1(self) -> None:
        print(self.event)
        self.context.price1 = await self.r1.get_price()
        self.context.price2 = await self.r2.get_price()

    @minion_step
    async def step_2(self) -> None:
        print(self.context)


minion = SimpleMultiResourcedMinion
