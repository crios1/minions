from tests.assets.events.record import RecordEvent
from tests.assets.support.pipeline_spied import SpiedPipeline


class RecordEventPipeline(SpiedPipeline[RecordEvent]):
    async def produce_event(self) -> RecordEvent:
        return RecordEvent(seq=1)


pipeline = RecordEventPipeline
