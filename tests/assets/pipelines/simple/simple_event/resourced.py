from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.resource_1 import SimpleResource1
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class SimpleResourcedPipeline(SubscriberReadyFixedEventsPipeline[SimpleEvent]):
    r1: SimpleResource1

    async def produce_event(self) -> SimpleEvent:
        return SimpleEvent(timestamp=0)


pipeline = SimpleResourcedPipeline
