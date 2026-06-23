from tests.assets.events.counter import CounterEvent
from tests.assets.resources.with_dependencies.depends_on_fixed import (
    AssetResource as ResourceDependingOnFixed,
)
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class AssetPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    depends_on_fixed_resource: ResourceDependingOnFixed

    async def produce_event(self) -> CounterEvent:
        return CounterEvent(seq=await self.depends_on_fixed_resource.get_value())


pipeline = AssetPipeline
