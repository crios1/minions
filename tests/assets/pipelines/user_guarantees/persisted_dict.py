import asyncio
import sys

from tests.assets.support.pipeline_spied import SpiedPipeline


class DictPersistenceGuaranteePipeline(SpiedPipeline[dict]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._emitted = False

    async def produce_event(self) -> dict:
        if self._emitted:
            await asyncio.sleep(sys.maxsize)

        while not self._mn_subs:
            await asyncio.sleep(0.01)

        self._emitted = True
        return {"kind": "dict-event", "payload": {"value": 10}}


pipeline = DictPersistenceGuaranteePipeline
