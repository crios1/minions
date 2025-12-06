import asyncio
import time
import sys

from minions import Pipeline

from .support.mixin_spy import SpyMixin
from .event_simple import SimpleEvent

class SimpleSingleEventPipeline1(SpyMixin, Pipeline[SimpleEvent]):
    total_events = 0

    async def produce_event(self) -> SimpleEvent:
        if type(self).total_events == 1:
            await asyncio.sleep(sys.maxsize)
        else:
            await asyncio.sleep(0.05)
            type(self).total_events += 1
        return SimpleEvent(timestamp=time.time())

# pipeline = SimpleSingleEventPipeline1