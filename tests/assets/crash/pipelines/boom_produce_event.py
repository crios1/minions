from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class BoomProduceEventPipeline(SpiedPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        await self.wait_for_subscribers()
        boom()
        return CounterEvent(seq=1)


pipeline = BoomProduceEventPipeline
