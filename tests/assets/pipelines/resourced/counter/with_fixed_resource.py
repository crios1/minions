from tests.assets.events.counter import CounterEvent
from tests.assets.resources.fixed.base import FixedResource
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class ResourcedPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    fixed_resource: FixedResource

    async def produce_event(self) -> CounterEvent:
        return CounterEvent(seq=await self.fixed_resource.get_value(0))


pipeline = ResourcedPipeline
