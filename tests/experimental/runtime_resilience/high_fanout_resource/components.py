import asyncio
from dataclasses import dataclass
from typing import ClassVar

from minions import Minion, Resource, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


@dataclass(frozen=True)
class CampaignConfig:
    subscriber_index: int


class SlowSharedResource(Resource):
    delay_seconds: ClassVar[float] = 0.01
    expected_calls: ClassVar[int] = 0
    calls_started: ClassVar[int] = 0
    calls_completed: ClassVar[int] = 0
    calls_inflight: ClassVar[int] = 0
    peak_calls_inflight: ClassVar[int] = 0
    all_calls_completed: ClassVar[asyncio.Event | None] = None

    @classmethod
    def reset(cls, *, expected_calls: int) -> None:
        cls.expected_calls = expected_calls
        cls.calls_started = 0
        cls.calls_completed = 0
        cls.calls_inflight = 0
        cls.peak_calls_inflight = 0
        cls.all_calls_completed = asyncio.Event()

    async def get_value(self) -> int:
        type(self).calls_started += 1
        type(self).calls_inflight += 1
        type(self).peak_calls_inflight = max(
            type(self).peak_calls_inflight,
            type(self).calls_inflight,
        )
        try:
            await asyncio.sleep(type(self).delay_seconds)
            return 123
        finally:
            type(self).calls_inflight -= 1
            type(self).calls_completed += 1
            completed = type(self).all_calls_completed
            if completed is not None and type(self).calls_completed == type(self).expected_calls:
                completed.set()


class FanoutPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    total_events = 1
    events_produced: ClassVar[int] = 0

    @classmethod
    def reset(cls, *, expected_subs: int, total_events: int) -> None:
        cls.configure_gate(expected_subs=expected_subs)
        cls.total_events = total_events
        cls.events_produced = 0

    async def produce_event(self) -> CounterEvent:
        type(self).events_produced += 1
        return CounterEvent(seq=type(self).events_produced)


class SlowResourceMinion(Minion[CounterEvent, CounterContext]):
    config: CampaignConfig
    slow_shared_resource: SlowSharedResource

    @minion_step
    async def handle(self) -> None:
        self.context.value = await self.slow_shared_resource.get_value()
        self.context.handled = True
