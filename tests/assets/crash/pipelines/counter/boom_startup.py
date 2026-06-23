from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class AssetPipeline(SpiedPipeline[CounterEvent]):
    async def startup(self) -> None:
        boom()

    async def produce_event(self) -> CounterEvent:
        return CounterEvent(seq=1)


pipeline = AssetPipeline
