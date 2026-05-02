import time

from .pipeline_fixed_event_count import FixedEventCountSpiedPipeline
from ..events.simple import SimpleEvent


class OverlapWindowPipeline(FixedEventCountSpiedPipeline[SimpleEvent]):
    first_emit_expected_subs = 1
    second_emit_expected_subs = 2
    total_events = 2

    @classmethod
    def reset_gate(
        cls,
        *,
        first_emit_expected_subs: int | None = None,
        second_emit_expected_subs: int | None = None,
    ) -> None:
        if first_emit_expected_subs is not None:
            cls.first_emit_expected_subs = first_emit_expected_subs
        if second_emit_expected_subs is not None:
            cls.second_emit_expected_subs = second_emit_expected_subs

    async def produce_event(self) -> SimpleEvent:
        if not hasattr(self, "_next_seq"):
            self._next_seq = 0
        expected_subs = (
            type(self).first_emit_expected_subs
            if self._next_seq == 0
            else type(self).second_emit_expected_subs
        )
        await self.wait_for_subscribers(expected_subs)

        self._next_seq += 1
        return SimpleEvent(timestamp=time.time())


pipeline = OverlapWindowPipeline
