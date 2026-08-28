import asyncio

import pytest

from minions import Minion, minion_step
from minions._internal._domain.exceptions import AbortWorkflow
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_ORCHESTRATION_ID,
    MINION_WORKFLOW_INFLIGHT_GAUGE,
)
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.support.race_window import GatedLock


def _workflow_inflight_gauge_value(
    minion: Minion[EmptyEvent, EmptyContext],
    metrics: InMemoryMetrics,
) -> float:
    return metrics.snapshot_gauge_value(
        MINION_WORKFLOW_INFLIGHT_GAUGE,
        {
            LABEL_ORCHESTRATION_ID: minion._mn_orchestration_id,
            LABEL_MINION: minion._mn_minion_id,
        },
    )


async def _wait_for_workflow_inflight_gauge_value(
    minion: Minion[EmptyEvent, EmptyContext],
    metrics: InMemoryMetrics,
    expected: float,
    *,
    timeout: float = 1.0,
) -> None:
    async def _wait() -> None:
        while _workflow_inflight_gauge_value(minion, metrics) != expected:
            await asyncio.sleep(0.01)

    await asyncio.wait_for(_wait(), timeout=timeout)


def _make_minion(
    minion_cls: type[Minion[EmptyEvent, EmptyContext]],
    *,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> Minion[EmptyEvent, EmptyContext]:
    return minion_cls(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=state_store,
        metrics=metrics,
        logger=logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )


@pytest.mark.asyncio
async def test_cancellation_before_workflow_registration_does_not_create_task(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    workflow_created = False

    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self) -> None:
            pass

    minion = _make_minion(
        MyMinion,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    tasks_gate = GatedLock()
    minion._mn_tasks_gate = tasks_gate

    async def workflow() -> None:
        nonlocal workflow_created
        workflow_created = True

    launch_task = asyncio.create_task(
        minion._mn_create_and_register_workflow_task_and_publish_inflight_gauge(
            workflow
        )
    )
    await tasks_gate.wait_until_held()

    assert not workflow_created
    assert not minion._mn_workflow_tasks
    assert not minion._mn_service_tasks

    launch_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await launch_task

    assert not tasks_gate.locked()
    assert not workflow_created
    assert not minion._mn_workflow_tasks
    assert not minion._mn_service_tasks


@pytest.mark.asyncio
async def test_tracks_concurrent_live_workflows(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    both_workflows_started = asyncio.Event()
    workflows_can_finish = (asyncio.Event(), asyncio.Event())
    workflows_started = 0

    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            nonlocal workflows_started
            workflow_index = workflows_started
            workflows_started += 1
            if workflows_started == 2:
                both_workflows_started.set()
            await workflows_can_finish[workflow_index].wait()

    minion = _make_minion(
        MyMinion,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    await minion._mn_startup()
    minion._mn_mark_running()

    assert _workflow_inflight_gauge_value(minion, metrics) == 0
    await minion._mn_handle_event(EmptyEvent())
    assert _workflow_inflight_gauge_value(minion, metrics) == 1

    await minion._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(both_workflows_started.wait(), timeout=1)

    assert _workflow_inflight_gauge_value(minion, metrics) == 2

    workflows_can_finish[0].set()
    await _wait_for_workflow_inflight_gauge_value(minion, metrics, 1)
    assert _workflow_inflight_gauge_value(minion, metrics) == 1

    workflows_can_finish[1].set()
    await minion._mn_wait_until_workflows_idle(timeout=2)

    assert _workflow_inflight_gauge_value(minion, metrics) == 0


@pytest.mark.asyncio
async def test_includes_startup_resumed_workflows(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    resume_started = asyncio.Event()
    can_finish = asyncio.Event()

    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            resume_started.set()
            await can_finish.wait()

    minion = _make_minion(
        MyMinion,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    await state_store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            orchestration_id=minion._mn_orchestration_id,
            workflow_id="resumed-workflow-id",
            event=EmptyEvent(),
            context=EmptyContext(),
        )
    )

    await minion._mn_startup()
    await asyncio.wait_for(resume_started.wait(), timeout=1)

    assert _workflow_inflight_gauge_value(minion, metrics) == 1

    can_finish.set()
    await minion._mn_wait_until_workflows_idle(timeout=2)

    assert _workflow_inflight_gauge_value(minion, metrics) == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["success", "failure", "abort", "cancellation"])
async def test_tracks_workflow_execution_lifecycle(
    outcome: str,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    step_started = asyncio.Event()
    workflow_can_end = asyncio.Event()

    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            step_started.set()
            await workflow_can_end.wait()
            if outcome == "failure":
                raise RuntimeError("boom")
            elif outcome == "abort":
                raise AbortWorkflow()

    minion = _make_minion(
        MyMinion,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    await minion._mn_startup()
    minion._mn_mark_running()

    assert _workflow_inflight_gauge_value(minion, metrics) == 0
    await minion._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(step_started.wait(), timeout=1)
    assert _workflow_inflight_gauge_value(minion, metrics) == 1

    if outcome == "cancellation":
        async with minion._mn_tasks_gate:
            workflow_task = next(iter(minion._mn_workflow_tasks))
        workflow_task.cancel()
    else:
        workflow_can_end.set()

    await minion._mn_wait_until_workflows_idle(timeout=2)

    assert _workflow_inflight_gauge_value(minion, metrics) == 0


@pytest.mark.asyncio
async def test_publishes_zero_on_shutdown(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    step_started = asyncio.Event()
    can_finish = asyncio.Event()

    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            step_started.set()
            await can_finish.wait()

    minion = _make_minion(
        MyMinion,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    minion._mn_mark_running()
    await minion._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(step_started.wait(), timeout=1)
    assert _workflow_inflight_gauge_value(minion, metrics) == 1

    await minion._mn_shutdown()

    assert _workflow_inflight_gauge_value(minion, metrics) == 0
