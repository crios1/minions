import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

import pytest

from minions import Minion, minion_step
from minions._internal._domain.gru import Gru
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.pipeline_triggered import TriggeredPipeline
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_orchestration_running,
    assert_runtime_empty,
)


async def _wait_until_event_acceptance_closes(
    minion: Minion[Any, Any],
    timeout: float = 1.0,
) -> None:
    async def wait() -> None:
        while minion._mn_accepting_events:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(wait(), timeout=timeout)


@pytest.mark.asyncio
async def test_rejects_unknown_mode(gru: Gru) -> None:
    with pytest.raises(ValueError, match="mode must be one of:"):
        await gru.stop_orchestration(
            "dummy-orchestration-id",
            mode="unknown",  # pyright: ignore[reportArgumentType]
        )


@pytest.mark.asyncio
async def test_rejects_force_with_drain(gru: Gru) -> None:
    with pytest.raises(ValueError, match="force cannot be used with mode='drain'"):
        await gru.stop_orchestration(
            "dummy-orchestration-id",
            mode="drain",
            force=True,
        )


@pytest.mark.asyncio
async def test_drain_waits_for_accepted_workflows_and_rejects_later_events(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    workflow_started = asyncio.Event()
    allow_workflow_to_finish = asyncio.Event()
    workflow_calls = 0

    class EmptyEventPipeline(TriggeredPipeline[EmptyEvent]):
        async def produce_event(self) -> EmptyEvent:
            return EmptyEvent()

    class WaitingMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def wait(self) -> None:
            nonlocal workflow_calls
            workflow_calls += 1
            workflow_started.set()
            await allow_workflow_to_finish.wait()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        started = await gru.start_orchestration(EmptyEventPipeline, WaitingMinion)
        assert started.success
        assert started.orchestration_id is not None
        orchestration = gru._orchestrations[started.orchestration_id]
        pipeline = orchestration.pipeline
        minion = orchestration.minion
        assert isinstance(pipeline, EmptyEventPipeline)

        await pipeline.trigger_event()
        await asyncio.wait_for(workflow_started.wait(), timeout=1.0)

        stop_task = asyncio.create_task(
            gru.stop_orchestration(started.orchestration_id, mode="drain")
        )
        await _wait_until_event_acceptance_closes(minion)

        assert not stop_task.done()
        await pipeline.trigger_event()
        await asyncio.sleep(0)
        assert workflow_calls == 1

        allow_workflow_to_finish.set()
        stopped = await stop_task

        assert stopped.success
        stopped_log = next(log for log in logger.logs if log.msg == "Orchestration stopped")
        assert stopped_log.kwargs["stop_mode"] == "drain"
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_cancelling_drain_reopens_event_acceptance(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    workflow_started = asyncio.Condition()
    allow_workflows_to_finish = asyncio.Event()
    workflow_calls = 0

    class EmptyEventPipeline(TriggeredPipeline[EmptyEvent]):
        async def produce_event(self) -> EmptyEvent:
            return EmptyEvent()

    class WaitingMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def wait(self) -> None:
            nonlocal workflow_calls
            async with workflow_started:
                workflow_calls += 1
                workflow_started.notify_all()
            await allow_workflows_to_finish.wait()

    async def wait_for_workflow_calls(count: int) -> None:
        async with workflow_started:
            await asyncio.wait_for(
                workflow_started.wait_for(lambda: workflow_calls >= count),
                timeout=1.0,
            )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        started = await gru.start_orchestration(EmptyEventPipeline, WaitingMinion)
        assert started.success
        assert started.orchestration_id is not None
        orchestration = gru._orchestrations[started.orchestration_id]
        pipeline = orchestration.pipeline
        minion = orchestration.minion
        assert isinstance(pipeline, EmptyEventPipeline)

        await pipeline.trigger_event()
        await wait_for_workflow_calls(1)

        drain_task = asyncio.create_task(
            gru.stop_orchestration(started.orchestration_id, mode="drain")
        )
        await _wait_until_event_acceptance_closes(minion)

        drain_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await drain_task

        assert minion._mn_accepting_events
        await assert_orchestration_running(gru, started.orchestration_id)

        await pipeline.trigger_event()
        await wait_for_workflow_calls(2)

        stopped = await gru.stop_orchestration(started.orchestration_id)
        assert stopped.success
