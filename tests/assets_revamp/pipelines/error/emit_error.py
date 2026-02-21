from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets_revamp.support.event_counter import CounterEvent


class ErrorPipeline(SpiedPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        raise RuntimeError("intentional pipeline error")


pipeline = ErrorPipeline
