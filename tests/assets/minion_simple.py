from minions import Minion, minion_step

from .support.mixin_spy import SpyMixin
from .event_simple import SimpleEvent

class SimpleMinion(SpyMixin, Minion[SimpleEvent, dict]):
    name = "simple-minion"

    @minion_step
    async def step_1(self):
        self.context['step1'] = "step1"

    @minion_step
    async def step_2(self):
        self.context['step2'] = "step2"
        print(self.context)

# minion = SimpleMinion

# NOTE: need to test that i can access self.event and self.context in all steps even when starting workflows from state store
# maybe it's best to make a specific test asset(s) for it that proves that access is working and especially after a crash