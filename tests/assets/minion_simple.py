from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from .event_simple import SimpleEvent

class SimpleMinion(SpiedMinion[SimpleEvent, dict]):
    name = "simple-minion"

    @minion_step
    async def step_1(self):
        self.context['step1'] = "step1"

    @minion_step
    async def step_2(self):
        self.context['step2'] = "step2"
        print(self.context)

minion = SimpleMinion
