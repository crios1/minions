from minions import minion_step

from .support.minion_spied import SpiedMinion
from .event_simple import SimpleEvent
from .resource_simple_1 import SimpleResource1

class SimpleResourcedMinion1(SpiedMinion[SimpleEvent, dict]):
    name = "simple-resourced-minion-1"
    simple_resource: SimpleResource1

    @minion_step
    async def step_1(self):
        if self.context.get('price'):
            return
        self.context['price'] = await self.simple_resource.get_price(self.event.timestamp)

    @minion_step
    async def step_2(self):
        print(self.context)

minion = SimpleResourcedMinion1

# TODO: need to test that i can access self.event in all steps even when starting workflows from state store
