import asyncio
import sys

from tests.assets.events.counter import CounterEvent
from tests.assets.resources.with_dependencies.depends_on_fixed import DependsOnFixedResource
from tests.assets.support.pipeline_spied import SpiedPipeline


class TransitiveResourcePipeline(SpiedPipeline[CounterEvent]):
    depends_on_fixed_resource: DependsOnFixedResource
    _emitted = False
    _expected_subs = 1

    @classmethod
    def reset_gate(cls, *, expected_subs: int | None = None) -> None:
        if expected_subs is not None:
            cls._expected_subs = expected_subs
        cls._emitted = False

    async def produce_event(self) -> CounterEvent:
        if type(self)._emitted:
            await asyncio.sleep(sys.maxsize)
        await self.wait_for_subscribers(type(self)._expected_subs)
        type(self)._emitted = True
        return CounterEvent(seq=await self.depends_on_fixed_resource.get_value())


pipeline = TransitiveResourcePipeline
