from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.events.record import RecordEvent


class RecordPipeline(SpiedPipeline[RecordEvent]):
    async def produce_event(self) -> RecordEvent:
        return RecordEvent()


pipeline = RecordPipeline
