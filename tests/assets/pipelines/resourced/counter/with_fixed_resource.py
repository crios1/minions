import asyncio
import sys

from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.base import FixedResource


class ResourcedPipeline(SpiedPipeline[CounterEvent]):
    fixed_resource: FixedResource
    _emitted = False
    expected_subs = 1

    @classmethod
    def reset_gate(cls, *, expected_subs: int | None = None) -> None:
        if expected_subs is not None:
            cls.expected_subs = expected_subs
        cls._emitted = False

    async def produce_event(self) -> CounterEvent:
        if type(self)._emitted:
            await asyncio.sleep(sys.maxsize)
        await self.wait_for_subscribers(type(self).expected_subs)
        value = await self.fixed_resource.get_value(0)
        type(self)._emitted = True
        return CounterEvent(seq=value)


pipeline = ResourcedPipeline
