import asyncio
import sys
import time

from .pipeline_spied import SpiedPipeline
from ..events.simple import SimpleEvent


class WaitForSubsPipeline(SpiedPipeline[SimpleEvent]):
    expected_subs = 1
    _emitted = False

    @classmethod
    def reset_gate(cls, *, expected_subs: int | None = None) -> None:
        if expected_subs is not None:
            cls.expected_subs = expected_subs
        cls._emitted = False

    async def produce_event(self) -> SimpleEvent:
        if type(self)._emitted:
            await asyncio.sleep(sys.maxsize)

        await self.wait_for_subscribers(type(self).expected_subs)

        type(self)._emitted = True
        return SimpleEvent(timestamp=time.time())


pipeline = WaitForSubsPipeline
