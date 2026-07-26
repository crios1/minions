import asyncio
from collections.abc import Coroutine
from typing import cast

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

    async def _mn_handle_event(self, event: RecordEvent) -> None:
        self.events.append(event)
        self.event_received.set()

    def _mn_identity_log_kwargs(self) -> dict[str, object]:
        return {
            "minion_instance_id": "dummy-minion-instance-id",
            "minion_id": "dummy-minion-id",
            "minion_config_id": "dummy-minion-config-id",
            "minion_module_path": "dummy.recording_subscriber",
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


def test_triggered_pipeline_has_no_autonomous_events():
    assert DummyPipeline.total_events == 0
