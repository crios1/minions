import time

from tests.assets.events.simple import SimpleEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class OverlapWindowPipeline(SubscriberReadyFixedEventsPipeline[SimpleEvent]):
    _first_emit_expected_subs = 1
    _second_emit_expected_subs = 2
    total_events = 2

    @classmethod
    def reset_gate(
        cls,
        *,
        expected_subs: int | None = None,
        first_emit_expected_subs: int | None = None,
        second_emit_expected_subs: int | None = None,
    ) -> None:
        super().reset_gate(expected_subs=expected_subs)
        if first_emit_expected_subs is not None:
            cls._first_emit_expected_subs = first_emit_expected_subs
        if second_emit_expected_subs is not None:
            cls._second_emit_expected_subs = second_emit_expected_subs

    async def wait_for_expected_subscribers(self) -> None:
        if not hasattr(self, "_next_seq"):
            self._next_seq = 0
        expected_subs = (
            type(self)._first_emit_expected_subs
            if self._next_seq == 0
            else type(self)._second_emit_expected_subs
        )
        await self.wait_for_subscribers(expected_subs)

    async def produce_event(self) -> SimpleEvent:
        if not hasattr(self, "_next_seq"):
            self._next_seq = 0
        self._next_seq += 1
        return SimpleEvent(timestamp=time.time())


pipeline = OverlapWindowPipeline
