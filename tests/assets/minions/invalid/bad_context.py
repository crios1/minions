from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.events.counter import CounterEvent


class UnserializableContext:
    pass


class BadContextMinion(SpiedMinion[CounterEvent, UnserializableContext]):
    name = "bad-context-minion"


minion = BadContextMinion
