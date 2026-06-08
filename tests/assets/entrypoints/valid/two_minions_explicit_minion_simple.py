from minions import Minion, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent
from tests.assets.events.simple import SimpleEvent


class Minion1(Minion[RecordEvent, CounterContext]):
    @minion_step
    async def step_1():
        ...


class Minion2(Minion[SimpleEvent, CounterContext]):
    @minion_step
    async def step_1():
        ...


minion = Minion2
