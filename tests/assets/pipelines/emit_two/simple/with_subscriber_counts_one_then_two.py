import time

from tests.assets.events.simple import SimpleEvent
from tests.assets.support.pipeline_subscriber_ready_events_by_emit import (
    SubscriberReadyEventsByEmitPipeline,
)


class AssetPipeline(SubscriberReadyEventsByEmitPipeline[SimpleEvent]):
    expected_subs_by_emit = (1, 2)

    async def produce_event(self) -> SimpleEvent:
        return SimpleEvent(timestamp=time.time())


pipeline = AssetPipeline
