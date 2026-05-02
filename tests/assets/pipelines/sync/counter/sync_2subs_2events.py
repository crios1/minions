from tests.assets.support.pipeline_fixed_event_count import FixedEventCountSpiedPipeline
from tests.assets.events.counter import CounterEvent


class SyncPipeline2Subs2Events(FixedEventCountSpiedPipeline[CounterEvent]):
    expected_subs = 2
    total_events = 2

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_seq = 0

    async def produce_event(self) -> CounterEvent:
        await self.wait_for_subscribers(type(self).expected_subs)
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = SyncPipeline2Subs2Events
