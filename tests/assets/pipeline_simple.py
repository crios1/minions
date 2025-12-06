import asyncio
import time

from .support.pipeline_spy import SpyPipeline
from .event_simple import SimpleEvent


class SimplePipeline(SpyPipeline[SimpleEvent]):

    async def produce_event(self) -> SimpleEvent:
        await asyncio.sleep(0.01)
        return SimpleEvent(timestamp=time.time())

# pipeline = SimplePipeline
