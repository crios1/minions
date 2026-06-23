from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)
from tests.assets.user_guarantees.persisted_shapes import StructEvent


class AssetPipeline(
    SubscriberReadyFixedEventsPipeline[StructEvent],
):
    async def produce_event(self) -> StructEvent:
        return StructEvent(kind="struct-event", payload_value=10)


pipeline = AssetPipeline
