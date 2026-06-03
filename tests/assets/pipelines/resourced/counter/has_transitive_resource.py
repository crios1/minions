from tests.assets.events.counter import CounterEvent
from tests.assets.resources.with_dependencies.depends_on_fixed import DependsOnFixedResource
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class TransitiveResourcePipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    depends_on_fixed_resource: DependsOnFixedResource

    async def produce_event(self) -> CounterEvent:
        return CounterEvent(seq=await self.depends_on_fixed_resource.get_value())


pipeline = TransitiveResourcePipeline
