from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.minion_spied import SpiedMinion


class SimpleMinion(SpiedMinion[SimpleEvent, SimpleContext]):

    @minion_step
    async def step_1(self) -> None:
        self.context.step1 = "step1"

    @minion_step
    async def step_2(self) -> None:
        self.context.step2 = "step2"
        print(self.context)


minion = SimpleMinion
