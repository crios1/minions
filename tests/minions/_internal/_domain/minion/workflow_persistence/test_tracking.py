import asyncio

import pytest

from minions import Minion, minion_step
from minions._internal._domain.minion import (
    WorkflowPersistenceFailurePolicy,
    WorkflowPersistenceState,
)
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_INFLIGHT_GAUGE,
)
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
)
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_noop import NoOpMinion
from tests.assets.support.state_store_failable import FailableStateStore
from tests.support.race_window import GatedLock


def _make_minion(
    *,
    minion_class: type[Minion[EmptyEvent, EmptyContext]],
    store: FailableStateStore,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    workflow_persistence_failure_policy: WorkflowPersistenceFailurePolicy = (
        "idle-until-persisted"
    ),
) -> Minion[EmptyEvent, EmptyContext]:
    return minion_class(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy=workflow_persistence_failure_policy,
        workflow_persistence_retry_delay_seconds=60.0,
        workflow_persistence_retry_max_delay_seconds=60.0,
        workflow_persistence_retry_jitter_ratio=0.0,
    )


def _make_workflow_context(
    minion: Minion[EmptyEvent, EmptyContext],
) -> MinionWorkflowContext[EmptyEvent, EmptyContext]:
    return MinionWorkflowContext(
        orchestration_id=minion._mn_orchestration_id,
        workflow_id="dummy-workflow-id",
        event=EmptyEvent(),
        context=EmptyContext(),
    )


async def _get_workflow_state(
    minion: Minion[EmptyEvent, EmptyContext],
    workflow_id: str,
) -> WorkflowPersistenceState:
    return (await minion._mn_workflow_persistence_state_snapshot())[workflow_id]


@pytest.mark.asyncio
async def test_event_acceptance_registers_missing_checkpoint_before_workflow_task_creation(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    minion._mn_mark_running()
    tasks_gate = GatedLock()
    minion._mn_tasks_gate = tasks_gate

    acceptance_task = asyncio.create_task(minion._mn_accept_event(EmptyEvent()))
    await tasks_gate.wait_until_held()

    states = await minion._mn_workflow_persistence_state_snapshot()
    assert len(states) == 1
    state = next(iter(states.values()))
    assert state.persisted_next_step_index is None
    assert state.next_step_index == 0
    assert state.risk_kind == "missing_checkpoint"
    assert not minion._mn_workflow_tasks

    acceptance_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await acceptance_task

    assert await minion._mn_workflow_persistence_state_snapshot() == {}


@pytest.mark.asyncio
async def test_persists_at_workflow_start_before_first_step_and_before_later_steps(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    first_step_started = asyncio.Event()
    continue_first_step = asyncio.Event()
    second_step_started = asyncio.Event()
    continue_second_step = asyncio.Event()

    class TwoStepMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def first_step(self):
            first_step_started.set()
            await continue_first_step.wait()

        @minion_step
        async def second_step(self):
            second_step_started.set()
            await continue_second_step.wait()

    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=TwoStepMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    minion._mn_mark_running()

    await minion._mn_accept_event(EmptyEvent())
    await asyncio.wait_for(first_step_started.wait(), timeout=1.0)

    assert len(store.saved_context_history) == 1
    initial_persisted_context = deserialize_workflow_context_blob(
        store.saved_context_history[0].context
    )
    assert initial_persisted_context.next_step_index == 0

    continue_first_step.set()
    await asyncio.wait_for(second_step_started.wait(), timeout=1.0)

    assert len(store.saved_context_history) == 2
    later_step_persisted_context = deserialize_workflow_context_blob(
        store.saved_context_history[1].context
    )
    assert later_step_persisted_context.next_step_index == 1

    continue_second_step.set()
    await minion._mn_wait_until_workflows_idle(timeout=1.0)


@pytest.mark.asyncio
async def test_register_new_workflow_persistence_state_rejects_duplicate_workflow_id(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)

    with pytest.raises(RuntimeError, match="already registered"):
        await minion._mn_register_new_workflow_persistence_state(context)

    state = await _get_workflow_state(minion, context.workflow_id)
    assert state.persisted_next_step_index is None
    assert state.next_step_index == 0


@pytest.mark.asyncio
async def test_workflow_start_save_failure_creates_missing_checkpoint_risk(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
        workflow_persistence_failure_policy="continue-on-failure",
    )
    context = _make_workflow_context(minion)
    store.save_failures.enable()
    await minion._mn_register_new_workflow_persistence_state(context)

    assert not await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )

    state = await _get_workflow_state(minion, context.workflow_id)
    assert state.persisted_next_step_index is None
    assert state.next_step_index == 0
    assert state.risk_kind == "missing_checkpoint"


@pytest.mark.asyncio
async def test_save_failure_after_workflow_advances_creates_stale_checkpoint_risk(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
        workflow_persistence_failure_policy="continue-on-failure",
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)
    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )
    context.next_step_index = 1
    store.save_failures.enable()

    assert not await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="before_step",
        step_name="second_step",
    )

    state = await _get_workflow_state(minion, context.workflow_id)
    assert state.persisted_next_step_index == 0
    assert state.next_step_index == 1
    assert state.risk_kind == "stale_checkpoint"


@pytest.mark.asyncio
async def test_save_success_after_stale_checkpoint_risk_clears_risk(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
        workflow_persistence_failure_policy="continue-on-failure",
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)
    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )
    context.next_step_index = 1
    store.save_failures.enable()
    assert not await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="before_step",
        step_name="second_step",
    )
    store.save_failures.disable()

    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="before_step",
        step_name="second_step",
    )

    state = await _get_workflow_state(minion, context.workflow_id)
    assert state.persisted_next_step_index == 1
    assert state.next_step_index == 1
    assert state.risk_kind is None


@pytest.mark.asyncio
async def test_delete_failure_creates_unresolved_delete_risk(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)
    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )
    store.delete_failures.enable()
    delete_task = asyncio.create_task(
        minion._mn_run_workflow_persistence_operation(
            context,
            persistence_point="workflow_resolve",
        )
    )
    await store.delete_failures.wait_for(1)

    state = await _get_workflow_state(minion, context.workflow_id)
    assert state.persisted_next_step_index == 0
    assert state.delete_pending
    assert state.risk_kind == "unresolved_delete"

    delete_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await delete_task


@pytest.mark.asyncio
async def test_workflow_completion_removes_workflow_persistence_state(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_entered = asyncio.Event()
    allow_step_to_finish = asyncio.Event()

    class WaitingMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def wait(self):
            step_entered.set()
            await allow_step_to_finish.wait()

    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=WaitingMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    minion._mn_mark_running()

    await minion._mn_accept_event(EmptyEvent())
    await asyncio.wait_for(step_entered.wait(), timeout=1.0)

    states = await minion._mn_workflow_persistence_state_snapshot()
    assert len(states) == 1
    assert next(iter(states.values())).risk_kind is None

    allow_step_to_finish.set()
    await minion._mn_wait_until_workflows_idle(timeout=1.0)

    assert await minion._mn_workflow_persistence_state_snapshot() == {}


@pytest.mark.asyncio
async def test_shutdown_before_workflow_task_admission_removes_workflow_persistence_state(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)
    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )

    minion._mn_shutting_down = True
    await minion._mn_run_workflow(context)

    assert not minion._mn_workflow_tasks
    assert await minion._mn_workflow_persistence_state_snapshot() == {}
    assert [stored.workflow_id for stored in await store.get_all_contexts()] == [
        context.workflow_id
    ]


@pytest.mark.asyncio
async def test_workflow_inflight_gauge_failure_cancels_workflow_and_removes_persistence_state(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    monkeypatch: pytest.MonkeyPatch,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    minion._mn_mark_running()
    set_metric = metrics._mn_set

    async def fail_inflight_gauge_publication(
        metric_name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        if metric_name == MINION_WORKFLOW_INFLIGHT_GAUGE:
            raise RuntimeError("controlled workflow inflight gauge failure")
        await set_metric(metric_name, value, labels)

    monkeypatch.setattr(metrics, "_mn_set", fail_inflight_gauge_publication)

    with pytest.raises(RuntimeError, match="controlled workflow inflight gauge failure"):
        await minion._mn_accept_event(EmptyEvent())

    assert not minion._mn_workflow_tasks
    assert not minion._mn_service_tasks
    assert await minion._mn_workflow_persistence_state_snapshot() == {}


@pytest.mark.asyncio
async def test_workflow_duration_metric_failure_removes_workflow_persistence_state(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    monkeypatch: pytest.MonkeyPatch,
):
    store = FailableStateStore(logger=logger)
    minion = _make_minion(
        minion_class=NoOpMinion,
        store=store,
        logger=logger,
        metrics=metrics,
    )
    minion._mn_mark_running()
    observe = metrics._mn_observe

    async def fail_workflow_duration_observation(
        metric_name: str,
        value: float,
        labels: dict[str, str] | None = None,
    ) -> None:
        if metric_name == MINION_WORKFLOW_DURATION_SECONDS:
            raise RuntimeError("controlled workflow duration metrics failure")
        await observe(metric_name, value, labels)

    monkeypatch.setattr(metrics, "_mn_observe", fail_workflow_duration_observation)

    await minion._mn_accept_event(EmptyEvent())
    await minion._mn_wait_until_all_tasks_idle(timeout=1.0)

    assert not minion._mn_workflow_tasks
    assert await minion._mn_workflow_persistence_state_snapshot() == {}
