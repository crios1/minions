from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class UnserializableContext:
    pass


class BadContextMinion(SpiedMinion[CounterEvent, UnserializableContext]):
    pass

minion = BadContextMinion
