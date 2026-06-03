from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline as _SubscriberReadyFixedEventsPipeline,
)


class SinglePipeline(_SubscriberReadyFixedEventsPipeline[CounterEvent]):
    async def produce_event(self) -> CounterEvent:
        return CounterEvent(seq=0)

del _SubscriberReadyFixedEventsPipeline
