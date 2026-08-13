from minions import Minion, minion_step
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent


class NoOpMinion(Minion[EmptyEvent, EmptyContext]):
    @minion_step
    async def step(self): ...
