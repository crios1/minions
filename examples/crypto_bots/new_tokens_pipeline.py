import asyncio
import time

from minions import Pipeline

class NewTokensPipeline(Pipeline):

    async def startup(self):
        self.interval = 60

    async def produce_event(self):
        await asyncio.sleep(self.interval)
        return {
            "timestamp": time.time(),
            "token": 'token'
        }
