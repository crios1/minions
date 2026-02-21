import asyncio
from tests.assets.support.pipeline_spied import SpiedPipeline
from .event_simple import SimpleEvent
from .resource_simple_1 import SimpleResource1

class SimpleResourcedPipeline(SpiedPipeline[SimpleEvent]):
    r1: SimpleResource1

    async def produce_event(self) -> SimpleEvent:
        await asyncio.sleep(0.01)
        return SimpleEvent(timestamp=0)

pipeline = SimpleResourcedPipeline
