from minions import Minion, minion_step

from .support.mixin_spy import SpyMixin
from .event_simple import SimpleEvent
from .resource_simple_1 import SimpleResource1
from .resource_simple_2 import SimpleResource2

class SimpleMultiResourcedMinion(SpyMixin, Minion[SimpleEvent, dict]):
    name = "simple-multi-resourced-minion"
    r1: SimpleResource1
    r2: SimpleResource2

    @minion_step
    async def step_1(self):
        print(self.event)
        self.context['price1'] = await self.r1.get_price()
        self.context['price2'] = await self.r2.get_price()

    @minion_step
    async def step_2(self):
        print(self.context)

# minion = SimpleMultiResourcedMinion
