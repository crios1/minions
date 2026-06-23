from tests.assets.events.simple import SimpleEvent
from tests.assets.resources.simple.default import AssetResource as SimpleResource
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class AssetPipeline(SubscriberReadyFixedEventsPipeline[SimpleEvent]):
    simple_resource: SimpleResource

    async def produce_event(self) -> SimpleEvent:
        return SimpleEvent(timestamp=0)


pipeline = AssetPipeline
