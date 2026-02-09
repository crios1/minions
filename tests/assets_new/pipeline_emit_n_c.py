import asyncio
import sys

from .event_counter import CounterEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class EmitNPipelineC(SpiedPipeline[CounterEvent]):
    expected_subs = 1
    total_events = 1
    _next_seq = 0

    @classmethod
    def reset_gate(cls, *, expected_subs: int | None = None, total_events: int | None = None) -> None:
        if expected_subs is not None:
            cls.expected_subs = expected_subs
        if total_events is not None:
            cls.total_events = total_events
        cls._next_seq = 0

    async def produce_event(self) -> CounterEvent:
        if type(self)._next_seq >= type(self).total_events:
            await asyncio.sleep(sys.maxsize)
        while True:
            async with self._mn_subs_lock:
                if len(self._mn_subs) >= type(self).expected_subs:
                    break
            await asyncio.sleep(0.01)
        seq = type(self)._next_seq
        type(self)._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = EmitNPipelineC
