from minions import Minion, minion_step

from tests.assets.events.simple import SimpleEvent


class Minion1(Minion[dict, dict]):
    @minion_step
    async def step_1():
        ...


class Minion2(Minion[SimpleEvent, dict]):
    @minion_step
    async def step_1():
        ...


minion = Minion2
