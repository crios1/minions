from tests.assets.contexts.counter import CounterContext
from tests.assets.support.minion_spied import SpiedMinion


class UnsupportedEventType:
    pass


class AssetMinion(SpiedMinion[UnsupportedEventType, CounterContext]):
    pass


minion = AssetMinion
