from minions import Minion, minion_step

from .support.mixin_spy import SpyMixin
from .event_simple import SimpleEvent
from .resource_simple_2 import SimpleResource2

class SimpleResourcedMinion2(SpyMixin, Minion[SimpleEvent, dict]):
    name = "simple-resourced-minion-2"
    simple_resource: SimpleResource2

    @minion_step
    async def step_1(self):
        print(self.event)
        self.context['price'] = await self.simple_resource.get_price()
        self.context['step1'] = "step1"

    @minion_step
    async def step_2(self):
        self.context['step2'] = "step2"
        print(self.context)

# minion = SimpleResourcedMinion2