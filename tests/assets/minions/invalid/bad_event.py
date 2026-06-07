from tests.assets.contexts.counter import CounterContext
from tests.assets.support.minion_spied import SpiedMinion


class UnserializableEvent:
    pass


class BadEventMinion(SpiedMinion[UnserializableEvent, CounterContext]):
    pass


minion = BadEventMinion
