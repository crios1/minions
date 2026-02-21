import asyncio
import sys
import time

from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.events.simple import SimpleEvent


class SimpleSingleEventPipeline2(SpiedPipeline[SimpleEvent]):
    total_events = 0

    async def produce_event(self) -> SimpleEvent:
        if type(self).total_events == 1:
            await asyncio.sleep(sys.maxsize)
        else:
            await asyncio.sleep(0.05)
            type(self).total_events += 1
        return SimpleEvent(timestamp=time.time())


pipeline = SimpleSingleEventPipeline2
