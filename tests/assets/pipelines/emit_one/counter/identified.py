from minions import pipeline_id
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


@pipeline_id("12345678-1234-5678-9234-567812345678")
class AssetPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    _next_seq = 0

    async def produce_event(self) -> CounterEvent:
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = AssetPipeline
