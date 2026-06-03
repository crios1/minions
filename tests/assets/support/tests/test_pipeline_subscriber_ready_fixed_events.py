import asyncio
from typing import cast

import pytest

from minions import Minion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class DummyPipeline(SubscriberReadyFixedEventsPipeline[RecordEvent]):
    async def produce_event(self) -> RecordEvent:
        return RecordEvent()


def make_pipeline() -> DummyPipeline:
    pipeline = object.__new__(DummyPipeline)
    pipeline._mn_subs = set()
    pipeline._mn_subs_lock = asyncio.Lock()
    return pipeline


@pytest.mark.asyncio
async def test_wait_for_subscribers_returns_when_expected_subscribers_present():
    pipeline = make_pipeline()
    pipeline._mn_subs.add(
        cast(Minion[RecordEvent, CounterContext], object.__new__(Minion))
    )
    await pipeline.wait_for_subscribers()


@pytest.mark.asyncio
async def test_wait_for_subscribers_times_out_with_observed_subscriber_count():
    pipeline = make_pipeline()
    with pytest.raises(
        TimeoutError,
        match=r"expected>=2, observed=0",
    ):
        await pipeline.wait_for_subscribers(expected_subs=2, timeout=0.001)
