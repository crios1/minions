from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext


class UnserializableEvent:
    pass


class BadEventMinion(SpiedMinion[UnserializableEvent, CounterContext]):
    name = "bad-event-minion"


minion = BadEventMinion
