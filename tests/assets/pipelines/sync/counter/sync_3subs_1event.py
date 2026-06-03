from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class SyncPipeline3Subs1Event(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    expected_subs = 3
    _next_seq = 0

    async def produce_event(self) -> CounterEvent:
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = SyncPipeline3Subs1Event
