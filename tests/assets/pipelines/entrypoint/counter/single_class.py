import asyncio
import sys

from tests.assets.events.counter import CounterEvent
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
        await self.wait_for_subscribers(type(self).expected_subs)
        type(self)._emitted = True
        return CounterEvent(seq=0)

del _SpiedPipeline
