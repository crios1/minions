from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.events.counter import CounterEvent


class FirstPipeline(SpiedPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        raise RuntimeError("should not run")

class SecondPipeline(SpiedPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        raise RuntimeError("should not run")
