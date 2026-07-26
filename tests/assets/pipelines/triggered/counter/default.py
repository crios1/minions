from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_triggered import TriggeredPipeline


class AssetPipeline(TriggeredPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        return CounterEvent(seq=0)


pipeline = AssetPipeline
