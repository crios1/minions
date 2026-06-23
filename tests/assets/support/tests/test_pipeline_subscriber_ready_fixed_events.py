import asyncio
from typing import cast

import pytest

from minions import Minion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent
from tests.assets.support.pipeline_subscriber_ready_events_by_emit import (
    SubscriberReadyEventsByEmitPipeline,
)
from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
    SubscriberReadyFixedEventsPipeline,
)


class DummyPipeline(SubscriberReadyFixedEventsPipeline[RecordEvent]):
    async def produce_event(self) -> RecordEvent:
        return RecordEvent()


class DummyEventsByEmitPipeline(SubscriberReadyEventsByEmitPipeline[RecordEvent]):
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


def test_configure_gate_by_emit_sets_gates_and_total_event_count():
    DummyEventsByEmitPipeline.configure_gate_by_emit(expected_subs_by_emit=(1, 2, 3))

    assert DummyEventsByEmitPipeline.expected_subs_by_emit == (1, 2, 3)
    assert DummyEventsByEmitPipeline.total_events == 3


def test_events_by_emit_pipeline_derives_total_event_count_from_class_gates():
    class ThreeEmitPipeline(SubscriberReadyEventsByEmitPipeline[RecordEvent]):
        expected_subs_by_emit = (1, 2, 3)

        async def produce_event(self) -> RecordEvent:
            return RecordEvent()

    assert ThreeEmitPipeline.total_events == 3


def test_events_by_emit_pipeline_rejects_global_expected_subscriber_count():
    with pytest.raises(
        TypeError,
        match="expected_subs_by_emit, not expected_subs",
    ):
        class InvalidPipeline(  # pyright: ignore[reportUnusedClass]
            SubscriberReadyEventsByEmitPipeline[RecordEvent]
        ):
            expected_subs = 2
            async def produce_event(self) -> RecordEvent:
                return RecordEvent()


def test_events_by_emit_pipeline_rejects_explicit_total_event_count():
    with pytest.raises(
        TypeError,
        match="derives total_events from expected_subs_by_emit",
    ):
        class InvalidPipeline(  # pyright: ignore[reportUnusedClass]
            SubscriberReadyEventsByEmitPipeline[RecordEvent]
        ):
            total_events = 2
            async def produce_event(self) -> RecordEvent:
                return RecordEvent()
