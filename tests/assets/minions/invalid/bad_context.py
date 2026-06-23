from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class UnserializableContext:
    pass


class AssetMinion(SpiedMinion[CounterEvent, UnserializableContext]):
    pass


minion = AssetMinion
