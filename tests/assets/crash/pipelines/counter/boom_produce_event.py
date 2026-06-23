from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class AssetPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        boom()
        return CounterEvent(seq=1)


pipeline = AssetPipeline
