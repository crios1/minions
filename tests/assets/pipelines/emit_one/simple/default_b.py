import time

from tests.assets.events.simple import SimpleEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class AssetPipeline(SubscriberReadyFixedEventsPipeline[SimpleEvent]):
    async def produce_event(self) -> SimpleEvent:
        return SimpleEvent(timestamp=time.time())


pipeline = AssetPipeline
