from tests.assets.support.pipeline_fixed_event_count import FixedEventCountSpiedPipeline
from tests.assets.events.counter import CounterEvent


class Emit1PipelineC(FixedEventCountSpiedPipeline[CounterEvent]):
    expected_subs = 1
    total_events = 1

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._next_seq = 0

    async def produce_event(self) -> CounterEvent:
        await self.wait_for_subscribers(type(self).expected_subs)
        seq = self._next_seq
        self._next_seq += 1
        return CounterEvent(seq=seq)


pipeline = Emit1PipelineC
