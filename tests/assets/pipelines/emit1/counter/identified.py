from minions import pipeline_id
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_fixed_event_count import FixedEventCountSpiedPipeline

IDENTIFIED_COUNTER_PIPELINE_ID = "12345678-1234-5678-9234-567812345678"


@pipeline_id(IDENTIFIED_COUNTER_PIPELINE_ID)
class IdentifiedEmit1Pipeline(FixedEventCountSpiedPipeline[CounterEvent]):
    expected_subs = 1
    total_events = 1
    _next_seq = 0

    async def produce_event(self) -> CounterEvent:
        await self.wait_for_subscribers(type(self).expected_subs)
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = IdentifiedEmit1Pipeline
