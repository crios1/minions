from tests.assets.crash.boom import boom
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class AssetPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    total_events = 0

    async def produce_event(self) -> CounterEvent:
        raise AssertionError("produce_event should not be called when total_events=0")

    async def shutdown(self) -> None:
        boom()


pipeline = AssetPipeline
