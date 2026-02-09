import asyncio
import sys

from .event_counter import CounterEvent
from tests.assets.support.pipeline_spied import SpiedPipeline as _SpiedPipeline


class SinglePipeline(_SpiedPipeline[CounterEvent]):
    expected_subs = 1
    _emitted = False

    @classmethod
    def reset_gate(cls, *, expected_subs: int | None = None) -> None:
        if expected_subs is not None:
            cls.expected_subs = expected_subs
        cls._emitted = False

    async def produce_event(self) -> CounterEvent:
        if type(self)._emitted:
            await asyncio.sleep(sys.maxsize)
        while True:
            async with self._mn_subs_lock:
                if len(self._mn_subs) >= type(self).expected_subs:
                    break
            await asyncio.sleep(0.01)
        type(self)._emitted = True
        return CounterEvent(seq=0)

del _SpiedPipeline
