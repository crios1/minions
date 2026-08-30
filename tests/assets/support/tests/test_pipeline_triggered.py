import asyncio
import contextlib
from collections.abc import Coroutine
from typing import cast
from unittest.mock import AsyncMock

import pytest

from minions import Minion
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent
from tests.assets.support.pipeline_triggered import TriggeredPipeline


class DummyPipeline(TriggeredPipeline[RecordEvent]):
    async def produce_event(self) -> RecordEvent:
        return RecordEvent()


class RecordingSubscriber:
    """Minimal Minion test double implementing only the Pipeline fanout surface."""

    def __init__(self) -> None:
        self._mn_orchestration_id = "dummy-orchestration-id"
        self.events: list[RecordEvent] = []
        self.tasks: list[asyncio.Task[None]] = []
        self.event_received = asyncio.Event()

    def safe_create_task(
        self,
        coro: Coroutine[object, object, object],
    ) -> asyncio.Task[None]:
        async def run() -> None:
            await coro

        task = asyncio.create_task(run())
        self.tasks.append(task)
        return task

    async def _mn_accept_event(self, event: RecordEvent) -> None:
        async def record_event() -> None:
            self.events.append(event)
            self.event_received.set()

        self.safe_create_task(record_event())

    def _mn_identity_log_kwargs(self) -> dict[str, object]:
        return {
            "minion_instance_id": "dummy-minion-instance-id",
            "minion_id": "dummy-minion-id",
            "minion_config_id": "dummy-minion-config-id",
            "minion_module_path": "dummy-minion-module-path",
        }


@pytest.mark.asyncio
async def test_trigger_waits_for_live_subscriber_before_emitting():
    pipeline = DummyPipeline(
        pipeline_id="dummy-pipeline-id",
        pipeline_module_path="dummy-pipeline-module-path",
        metrics=NoOpMetrics(),
        logger=NoOpLogger(),
    )
    subscriber = RecordingSubscriber()

    trigger_task = asyncio.create_task(pipeline.trigger_event())
    await asyncio.sleep(0)

    assert not trigger_task.done()
    assert not subscriber.events

    await pipeline._mn_subscribe(
        cast(Minion[RecordEvent, CounterContext], subscriber)
    )
    await asyncio.wait_for(trigger_task, timeout=1.0)
    await asyncio.wait_for(subscriber.event_received.wait(), timeout=1.0)
    await asyncio.gather(*subscriber.tasks)

    assert subscriber.events == [RecordEvent()]


@pytest.mark.asyncio
async def test_run_does_not_produce_events_without_explicit_trigger(
    monkeypatch: pytest.MonkeyPatch,
):
    pipeline = DummyPipeline(
        pipeline_id="dummy-pipeline-id",
        pipeline_module_path="dummy-pipeline-module-path",
        metrics=NoOpMetrics(),
        logger=NoOpLogger(),
    )
    subscriber = RecordingSubscriber()
    await pipeline._mn_subscribe(
        cast(Minion[RecordEvent, CounterContext], subscriber)
    )
    produce_event = AsyncMock(return_value=RecordEvent())
    monkeypatch.setattr(pipeline, "produce_event", produce_event)

    run_task = asyncio.create_task(pipeline.run())
    try:
        await asyncio.sleep(0)

        assert not run_task.done()
        produce_event.assert_not_awaited()
        assert not subscriber.events
    finally:
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
