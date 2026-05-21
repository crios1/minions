from minions import Minion

from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent


class Minion1(Minion[RecordEvent, CounterContext]):
    ...


class Minion2(Minion[RecordEvent, CounterContext]):
    ...
