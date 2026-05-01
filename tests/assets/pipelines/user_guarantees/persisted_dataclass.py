import asyncio
import sys

from tests.assets.user_guarantees.persisted_shapes import DataclassEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class DataclassPersistenceGuaranteePipeline(SpiedPipeline[DataclassEvent]):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._emitted = False

    async def produce_event(self) -> DataclassEvent:
        if self._emitted:
            await asyncio.sleep(sys.maxsize)

        while not self._mn_subs:
            await asyncio.sleep(0.01)

        self._emitted = True
        return DataclassEvent(kind="dataclass-event", payload_value=10)


pipeline = DataclassPersistenceGuaranteePipeline
