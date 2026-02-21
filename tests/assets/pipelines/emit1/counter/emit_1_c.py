import asyncio
import sys

from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.events.counter import CounterEvent


class Emit1PipelineC(SpiedPipeline[CounterEvent]):
    expected_subs = 1
    total_events = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_seq = 0

    async def produce_event(self) -> CounterEvent:
        if self._next_seq >= type(self).total_events:
            await asyncio.sleep(sys.maxsize)
        while True:
            async with self._mn_subs_lock:
                if len(self._mn_subs) >= type(self).expected_subs:
                    break
            await asyncio.sleep(0.01)
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = Emit1PipelineC
