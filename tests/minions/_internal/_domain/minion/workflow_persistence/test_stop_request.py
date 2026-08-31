import asyncio

import pytest

from minions._internal._domain.minion import WorkflowPersistenceFailurePolicy
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_noop import NoOpMinion
from tests.assets.support.state_store_failable import FailableStateStore


def _make_minion(
    *,
    store: FailableStateStore,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    policy: WorkflowPersistenceFailurePolicy,
) -> NoOpMinion:
    return NoOpMinion(
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
        workflow_persistence_failure_policy=policy,
        workflow_persistence_retry_delay_seconds=60.0,
        workflow_persistence_retry_max_delay_seconds=60.0,
        workflow_persistence_retry_jitter_ratio=0.0,
    )


def _make_workflow_context(
    minion: NoOpMinion,
) -> MinionWorkflowContext[EmptyEvent, EmptyContext]:
    return MinionWorkflowContext(
        orchestration_id=minion._mn_orchestration_id,
        workflow_id="dummy-workflow-id",
        event=EmptyEvent(),
        context=EmptyContext(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy",
    ["continue-on-failure", "idle-until-persisted"],
)
async def test_requires_force_when_workflow_persistence_risk_exists(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    policy: WorkflowPersistenceFailurePolicy,
) -> None:
    minion = _make_minion(
        store=FailableStateStore(logger=logger),
        logger=logger,
        metrics=metrics,
        policy=policy,
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)

    stop_accepted, stop_risks = await minion._mn_request_stop()

    assert not stop_accepted
    assert len(stop_risks) == 1
    assert stop_risks[0].workflow_id == context.workflow_id
    assert stop_risks[0].kind == "missing_checkpoint"
    assert not minion._mn_shutting_down

    forced_stop_accepted, forced_stop_risks = await minion._mn_request_stop(
        force=True
    )

    assert forced_stop_accepted
    assert forced_stop_risks == stop_risks
    assert minion._mn_shutting_down
    assert not await minion._mn_accept_event(EmptyEvent())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "policy",
    ["continue-on-failure", "idle-until-persisted"],
)
async def test_is_accepted_when_no_workflow_persistence_risk_exists(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    policy: WorkflowPersistenceFailurePolicy,
) -> None:
    minion = _make_minion(
        store=FailableStateStore(logger=logger),
        logger=logger,
        metrics=metrics,
        policy=policy,
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)
    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )

    stop_accepted, stop_risks = await minion._mn_request_stop()

    assert stop_accepted
    assert stop_risks == ()


@pytest.mark.asyncio
async def test_rejected_request_is_accepted_after_workflow_persistence_succeeds(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
) -> None:
    minion = _make_minion(
        store=FailableStateStore(logger=logger),
        logger=logger,
        metrics=metrics,
        policy="continue-on-failure",
    )
    context = _make_workflow_context(minion)
    await minion._mn_register_new_workflow_persistence_state(context)

    stop_accepted, stop_risks = await minion._mn_request_stop()
    assert not stop_accepted
    assert stop_risks[0].kind == "missing_checkpoint"

    assert await minion._mn_run_workflow_persistence_operation(
        context,
        persistence_point="workflow_start",
    )

    stop_accepted, stop_risks = await minion._mn_request_stop()
    assert stop_accepted
    assert stop_risks == ()


@pytest.mark.asyncio
async def test_rejects_when_event_is_accepted_concurrently(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
) -> None:
    store = FailableStateStore(logger=logger)
    store.save_failures.enable()
    minion = _make_minion(
        store=store,
        logger=logger,
        metrics=metrics,
        policy="continue-on-failure",
    )
    minion._mn_mark_running()
    await minion._mn_event_acceptance_lock.acquire()
    acceptance_task = asyncio.create_task(minion._mn_accept_event(EmptyEvent()))
    await asyncio.sleep(0)
    stop_request_task = asyncio.create_task(minion._mn_request_stop())
    await asyncio.sleep(0)

    minion._mn_event_acceptance_lock.release()

    assert await acceptance_task
    stop_accepted, stop_risks = await stop_request_task
    assert not stop_accepted
    assert len(stop_risks) == 1
    assert stop_risks[0].kind == "missing_checkpoint"

    await minion._mn_request_stop(force=True)
    await minion._mn_shutdown()
