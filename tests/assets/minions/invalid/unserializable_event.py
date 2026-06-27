import threading
from dataclasses import dataclass

from tests.assets.contexts.counter import CounterContext
from tests.assets.support.minion_spied import SpiedMinion


@dataclass
class UnserializableEvent:
    lock: threading.Lock


class AssetMinion(SpiedMinion[UnserializableEvent, CounterContext]):
    pass


minion = AssetMinion
