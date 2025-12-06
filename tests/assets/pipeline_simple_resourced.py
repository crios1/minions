import asyncio
from minions import Pipeline

from .support.mixin_spy import SpyMixin
from .event_simple import SimpleEvent
from .resource_simple_1 import SimpleResource1

class SimpleResourcedPipeline(SpyMixin, Pipeline[SimpleEvent]):
    r1: SimpleResource1

    async def produce_event(self) -> SimpleEvent:
        await asyncio.sleep(0.01)
        return SimpleEvent(timestamp=0)

# pipeline = PipelineResourced
