import threading
from dataclasses import dataclass

from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


@dataclass
class UnserializableContext:
    lock: threading.Lock


class AssetMinion(SpiedMinion[CounterEvent, UnserializableContext]):
    pass


minion = AssetMinion
