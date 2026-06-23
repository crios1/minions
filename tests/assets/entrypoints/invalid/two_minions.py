from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.minion_spied import SpiedMinion


class FirstMinion(SpiedMinion[SimpleEvent, SimpleContext]):
    @minion_step
    async def step_1(self) -> None:
        return


class SecondMinion(SpiedMinion[SimpleEvent, SimpleContext]):
    @minion_step
    async def step_1(self) -> None:
        return
