import contextlib
from collections.abc import Callable
from pathlib import Path

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.metrics_constants import (
    LABEL_ERROR_TYPE,
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_ORCHESTRATION_ID,
    LABEL_PIPELINE,
    LABEL_RESOURCE,
    LABEL_RESOURCE_CALLER,
    LABEL_RESOURCE_CALLER_KIND,
    LABEL_RESOURCE_METHOD,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    PIPELINE_ERROR_TOTAL,
    RESOURCE_ERROR_TOTAL,
)
from minions._internal._framework.minion_workflow_context_codec import (
    serialize_persisted_workflow_context,
)
from minions._internal._framework.state_store import StoredWorkflowContext
from tests.assets.contexts.simple import SimpleContext
from tests.assets.crash.boom import BOOM_MESSAGE
from tests.assets.crash.resources.boom_method import AssetResource as BoomMethodResource
from tests.assets.crash.support.state_store.boom_get_contexts_for_orchestration import (
    AssetStateStore as BoomGetContextsForOrchestrationStateStore,
)
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import assert_runtime_empty


def orchestration_id(pipeline_module_path: str, minion_module_path: str, config: str = "") -> str:
    return Gru._make_orchestration_id(
        pipeline_id=pipeline_module_path,
        minion_id=minion_module_path,
        minion_config_id=config,
    )


async def assert_gru_can_start_and_stop_known_good_orchestration(gru: Gru) -> None:
    result = await gru.start_orchestration(
        "tests.assets.crash.pipelines.counter.healthy",
        "tests.assets.crash.minions.counter.healthy",
    )
    assert result.success
    assert result.orchestration_id is not None
    assert isinstance(gru._logger, InMemoryLogger)
    assert await gru._logger.wait_for_log("Workflow succeeded", timeout=1.0)
    stop = await gru.stop_orchestration(result.orchestration_id)
    assert stop.success
    assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_orchestration_contains_state_store_resume_read_failure(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
) -> None:
    state_store = BoomGetContextsForOrchestrationStateStore(logger=logger)

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.healthy",
        )

        assert not result.success
        assert (
            result.reason
            == "tests.assets.crash.minions.counter.healthy.AssetMinion.startup failed"
        )
        assert logger.has_log(
            "AssetStateStore.get_contexts_for_orchestration failed",
            log_kwargs={"error_type": "BoomError"},
        )
        assert logger.has_log(
            "Failed to start orchestration",
            log_kwargs={
                "error_type": "MinionsError",
                "cause_error_type": "BoomError",
                "cause_error_message": BOOM_MESSAGE,
            },
        )
        assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_orchestration_fails_closed_on_persisted_workflow_decode_mismatch(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    expected_orchestration_id = orchestration_id(
        "tests.assets.crash.pipelines.counter.healthy",
        "tests.assets.crash.minions.counter.healthy",
    )
    persisted_context = MinionWorkflowContext(
        orchestration_id=expected_orchestration_id,
        workflow_id="wf-incompatible",
        event=SimpleEvent(timestamp=1.0),
        context=SimpleContext(value=1),
        next_step_index=0,
    )
    state_store._contexts["wf-incompatible"] = StoredWorkflowContext(
        workflow_id="wf-incompatible",
        orchestration_id=expected_orchestration_id,
        context=serialize_persisted_workflow_context(persisted_context),
    )

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.healthy",
        )

        assert not result.success
        assert result.reason is not None
        assert (
            "could not be decoded with the current Minion event and workflow context types"
            in result.reason
        )
        assert result.suggestion is not None
        assert "drain the orchestration" in result.suggestion
        assert (
            "delete the persisted workflow contexts for orchestration "
            f"{expected_orchestration_id!r}"
            in result.suggestion
        )
        assert logger.has_log(
            "StateStore failed to decode stored workflow context",
            log_kwargs={
                "workflow_id": "wf-incompatible",
                "error_type": "WorkflowContextTypeMismatchError",
            },
        )
        assert logger.has_log("Failed to start orchestration")
        assert_runtime_empty(gru)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pipeline_module_path", "minion_module_path"),
    [
        (
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.boom_startup",
        ),
        (
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.boom_load_config",
        ),
        (
            "tests.assets.crash.pipelines.counter.boom_startup",
            "tests.assets.crash.minions.counter.healthy",
        ),
        (
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.with_boom_startup_resource",
        ),
    ],
)
async def test_start_orchestration_contains_user_code_startup_failures(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    pipeline_module_path: str,
    minion_module_path: str,
    tests_dir: Path,
) -> None:
    config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            minion=minion_module_path,
            pipeline=pipeline_module_path,
            minion_config_path=config_path if "boom_load_config" in minion_module_path else None,
        )

        assert not result.success
        assert logger.has_log(
            "Failed to start orchestration",
            log_kwargs={
                "error_type": "MinionsError",
                "cause_error_type": "BoomError",
                "cause_error_message": BOOM_MESSAGE,
            },
        )
        assert_runtime_empty(gru)
        await assert_gru_can_start_and_stop_known_good_orchestration(gru)


@pytest.mark.asyncio
async def test_minion_step_failure_is_logged_measured_and_contained(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        healthy_counter_pipeline_id = gru._get_pipeline_identity_from_module_path(
            "tests.assets.crash.pipelines.counter.healthy",
        )
        boom_step_minion_id = gru._get_minion_identity_from_module_path(
            "tests.assets.crash.minions.counter.boom_step",
        )
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=healthy_counter_pipeline_id,
            minion_id=boom_step_minion_id,
            minion_config_id="",
        )

        result = await gru.start_orchestration(
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.boom_step",
        )
        assert result.success

        assert await logger.wait_for_log(
            "Workflow failed",
            log_kwargs={"error_type": "BoomError"},
            timeout=1.0,
        )
        assert metrics.snapshot_counter_value(
            MINION_WORKFLOW_STEP_FAILED_TOTAL,
            {
                LABEL_ORCHESTRATION_ID: expected_orchestration_id,
                LABEL_MINION: boom_step_minion_id,
                LABEL_MINION_WORKFLOW_STEP: "step_1",
                LABEL_ERROR_TYPE: "BoomError",
            },
        ) >= 1
        assert metrics.snapshot_counter_value(
            MINION_WORKFLOW_FAILED_TOTAL,
            {
                LABEL_ORCHESTRATION_ID: expected_orchestration_id,
                LABEL_MINION: boom_step_minion_id,
                LABEL_ERROR_TYPE: "BoomError",
            },
        ) >= 1
        stop = await gru.stop_orchestration(result.orchestration_id or "")
        assert stop.success
        assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_pipeline_produce_event_failure_is_logged_measured_and_shutdown_is_clean(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        boom_produce_event_pipeline_id = gru._get_pipeline_identity_from_module_path(
            "tests.assets.crash.pipelines.counter.boom_produce_event",
        )

        result = await gru.start_orchestration(
            "tests.assets.crash.pipelines.counter.boom_produce_event",
            "tests.assets.crash.minions.counter.healthy",
        )
        assert result.success

        assert await logger.wait_for_log(
            "Gru runtime task failure observed",
            log_kwargs={"component": "pipeline"},
            timeout=1.0,
        )
        assert metrics.snapshot_counter_value(
            PIPELINE_ERROR_TOTAL,
            {
                LABEL_PIPELINE: boom_produce_event_pipeline_id,
                LABEL_ERROR_TYPE: "BoomError",
            },
        ) >= 1
        shutdown = await gru.shutdown()
        assert shutdown.success


@pytest.mark.asyncio
async def test_resource_method_failure_is_logged_measured_and_contained(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        healthy_counter_pipeline_id = gru._get_pipeline_identity_from_module_path(
            "tests.assets.crash.pipelines.counter.healthy",
        )
        with_boom_method_resource_minion_id = gru._get_minion_identity_from_module_path(
            "tests.assets.crash.minions.counter.with_boom_method_resource",
        )
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=healthy_counter_pipeline_id,
            minion_id=with_boom_method_resource_minion_id,
            minion_config_id="",
        )

        boom_method_resource_id = gru._get_resource_identity(BoomMethodResource)

        result = await gru.start_orchestration(
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.with_boom_method_resource",
        )
        assert result.success

        assert await logger.wait_for_log(
            "Workflow failed",
            log_kwargs={"error_type": "BoomError"},
            timeout=1.0,
        )
        resource_failed = logger.find_first_log("Resource method failed")
        assert resource_failed is not None
        assert resource_failed.kwargs["error_type"] == "BoomError"
        assert resource_failed.kwargs["resource_method"] == "explode"
        assert metrics.snapshot_counter_value(
            RESOURCE_ERROR_TOTAL,
            {
                LABEL_RESOURCE: boom_method_resource_id,
                LABEL_RESOURCE_METHOD: "explode",
                LABEL_RESOURCE_CALLER_KIND: "minion",
                LABEL_RESOURCE_CALLER: with_boom_method_resource_minion_id,
                LABEL_ORCHESTRATION_ID: expected_orchestration_id,
                LABEL_ERROR_TYPE: "BoomError",
            },
        ) >= 1
        stop = await gru.stop_orchestration(result.orchestration_id or "")
        assert stop.success
        assert_runtime_empty(gru)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("pipeline_module_path", "minion_module_path"),
    [
        (
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.boom_shutdown",
        ),
        (
            "tests.assets.crash.pipelines.counter.blocking_boom_shutdown",
            "tests.assets.crash.minions.counter.healthy",
        ),
        (
            "tests.assets.crash.pipelines.counter.healthy",
            "tests.assets.crash.minions.counter.with_boom_shutdown_resource",
        ),
    ],
)
async def test_shutdown_failures_are_reported_and_singleton_is_released(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    pipeline_module_path: str,
    minion_module_path: str,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(pipeline_module_path, minion_module_path)
        assert result.success
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        if minion_module_path.endswith(".boom_shutdown"):
            assert not stop.success
            assert logger.has_log("Failed to stop orchestration")
        else:
            assert stop.success
            assert logger.has_log("shutdown failed during startup error recovery")
    
        assert_runtime_empty(gru)

    # The factory shutdown must release the global singleton even after a failed stop path.
    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as fresh_gru:
        await assert_gru_can_start_and_stop_known_good_orchestration(fresh_gru)
