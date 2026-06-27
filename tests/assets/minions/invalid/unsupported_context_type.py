from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class UnsupportedContextType:
    pass


class AssetMinion(SpiedMinion[CounterEvent, UnsupportedContextType]):
    pass


minion = AssetMinion
