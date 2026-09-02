import asyncio
import contextlib
from unittest.mock import AsyncMock

import pytest

from minions import Minion, minion_step
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.minion_noop import NoOpMinion
from tests.assets.support.pipeline_triggered import TriggeredPipeline


class EmptyEventPipeline(TriggeredPipeline[EmptyEvent]):
    async def produce_event(self) -> EmptyEvent:
        return EmptyEvent()


def _make_minion(
    minion_class: type[Minion[EmptyEvent, EmptyContext]],
) -> Minion[EmptyEvent, EmptyContext]:
    return minion_class(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=NoOpStateStore(),
        metrics=NoOpMetrics(),
        logger=NoOpLogger(),
        minion_id="dummy-minion-id",
        minion_config_id="dummy-minion-config-id",
        pipeline_id="dummy-pipeline-id",
    )


@pytest.mark.asyncio
async def test_trigger_waits_for_live_minion_before_emitting():
    events: list[EmptyEvent] = []

    class RecordingMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def record_event(self) -> None:
            events.append(self.event)

    pipeline = EmptyEventPipeline(
        pipeline_id="dummy-pipeline-id",
        pipeline_module_path="dummy-pipeline-module-path",
        metrics=NoOpMetrics(),
        logger=NoOpLogger(),
    )
    minion = _make_minion(RecordingMinion)
    minion._mn_mark_running()

    emit_event_task = asyncio.create_task(
        pipeline.wait_for_subscribers_then_emit_event()
    )
    await asyncio.sleep(0)

    assert not emit_event_task.done()
    assert not events

    await pipeline._mn_subscribe(minion)
    await asyncio.wait_for(emit_event_task, timeout=1.0)
    await minion._mn_wait_until_workflows_idle()

    assert events == [EmptyEvent()]


@pytest.mark.asyncio
async def test_run_does_not_produce_events_without_explicit_trigger(
    monkeypatch: pytest.MonkeyPatch,
):
    pipeline = EmptyEventPipeline(
        pipeline_id="dummy-pipeline-id",
        pipeline_module_path="dummy-pipeline-module-path",
        metrics=NoOpMetrics(),
        logger=NoOpLogger(),
    )
    minion = _make_minion(NoOpMinion)
    await pipeline._mn_subscribe(minion)
    produce_event = AsyncMock(return_value=EmptyEvent())
    monkeypatch.setattr(pipeline, "produce_event", produce_event)

    run_task = asyncio.create_task(pipeline.run())
    try:
        await asyncio.sleep(0)

        assert not run_task.done()
        produce_event.assert_not_awaited()
    finally:
        run_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await run_task
