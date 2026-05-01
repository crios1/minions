import asyncio
import sys

from tests.assets.user_guarantees.persisted_shapes import StructEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class StructPersistenceGuaranteePipeline(SpiedPipeline[StructEvent]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._emitted = False

    async def produce_event(self) -> StructEvent:
        if self._emitted:
            await asyncio.sleep(sys.maxsize)

        while not self._mn_subs:
            await asyncio.sleep(0.01)

        self._emitted = True
        return StructEvent(kind="struct-event", payload_value=10)


pipeline = StructPersistenceGuaranteePipeline
