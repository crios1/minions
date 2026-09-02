from minions._internal._domain.types import T_Event

from .pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class TriggeredPipeline(
    SubscriberReadyFixedEventsPipeline[T_Event],
    defer_pipeline_setup=True,
):
    """Test pipeline that emits events only when explicitly triggered."""

    total_events = 0

    async def wait_for_subscribers_then_emit_event(self) -> None:
        await self.wait_for_expected_subscribers()
        await self._mn_produce_and_fan_out_event()
