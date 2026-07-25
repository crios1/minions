import asyncio
from dataclasses import dataclass
from typing import Any, ClassVar

from minions import Minion, Resource, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


@dataclass(frozen=True)
class CampaignConfig:
    subscriber_index: int


class GatedSharedResource(Resource):
    expected_calls: ClassVar[int] = 0
    calls_started: ClassVar[int] = 0
    all_calls_started: ClassVar[asyncio.Event | None] = None
    release_calls: ClassVar[asyncio.Event | None] = None

    @classmethod
    def reset(cls, *, expected_calls: int) -> None:
        cls.expected_calls = expected_calls
        cls.calls_started = 0
        cls.all_calls_started = asyncio.Event()
        cls.release_calls = asyncio.Event()

    @classmethod
    def release(cls) -> None:
        assert cls.release_calls is not None
        cls.release_calls.set()

    async def get_value(self) -> int:
        type(self).calls_started += 1
        if type(self).calls_started == type(self).expected_calls:
            all_calls_started = type(self).all_calls_started
            assert all_calls_started is not None
            all_calls_started.set()
        release_calls = type(self).release_calls
        assert release_calls is not None
        await release_calls.wait()
        return 123


class BurstPipeline(SubscriberReadyFixedEventsPipeline[CounterEvent]):
    events_produced: ClassVar[int] = 0

    @classmethod
    def reset(cls, *, expected_subs: int, total_events: int) -> None:
        cls.configure_gate(expected_subs=expected_subs)
        cls.total_events = total_events
        cls.events_produced = 0

    async def produce_event(self) -> CounterEvent:
        type(self).events_produced += 1
        return CounterEvent(seq=type(self).events_produced)


class GatedResourceMinion(Minion[CounterEvent, CounterContext]):
    config: CampaignConfig
    gated_shared_resource: GatedSharedResource

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        setattr(self, "_mn_shutdown_grace_seconds", 0.01)

    @minion_step
    async def handle(self) -> None:
        self.context.value = await self.gated_shared_resource.get_value()
        self.context.handled = True
