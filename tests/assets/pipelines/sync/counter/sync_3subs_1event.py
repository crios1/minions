import asyncio
import sys

from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.events.counter import CounterEvent


class SyncPipeline3Subs1Event(SpiedPipeline[CounterEvent]):
    expected_subs = 3
    total_events = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_seq = 0

    async def produce_event(self) -> CounterEvent:
        if self._next_seq >= type(self).total_events:
            await asyncio.sleep(sys.maxsize)
        await self.wait_for_subscribers(type(self).expected_subs)
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = SyncPipeline3Subs1Event
