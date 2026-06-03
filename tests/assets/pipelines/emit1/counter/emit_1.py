from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class Emit1Pipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    _next_seq = 0

    async def produce_event(self) -> CounterEvent:
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = Emit1Pipeline
