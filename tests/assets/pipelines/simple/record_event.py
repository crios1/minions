from tests.assets.events.record import RecordEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class RecordPipeline(SubscriberReadyFixedEventsPipeline[RecordEvent]):
    async def produce_event(self) -> RecordEvent:
        return RecordEvent()


pipeline = RecordPipeline
