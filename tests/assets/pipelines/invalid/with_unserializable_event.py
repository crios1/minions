import threading
from dataclasses import dataclass

from tests.assets.support.pipeline_spied import SpiedPipeline


@dataclass
class UnserializableEvent:
    lock: threading.Lock


class AssetPipeline(SpiedPipeline[UnserializableEvent]):
    async def produce_event(self):
        raise RuntimeError("should not run")


pipeline = AssetPipeline
