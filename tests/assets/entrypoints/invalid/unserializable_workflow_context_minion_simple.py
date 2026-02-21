import threading
from dataclasses import dataclass, field

from minions import Minion, minion_step

from tests.assets.events.simple import SimpleEvent


@dataclass
class UnserializableWFCTX:
    lock: threading.Lock = field(default_factory=threading.Lock, repr=False, compare=False)


class UnserializableWFCTXMinion(Minion[SimpleEvent, UnserializableWFCTX]):
    @minion_step
    async def step_1(self):
        print(self.event)
        print(self.context.lock)
