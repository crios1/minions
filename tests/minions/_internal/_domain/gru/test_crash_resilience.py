import contextlib
from collections.abc import Callable
from pathlib import Path

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import serialize_persisted_workflow_context
from minions._internal._framework.state_store import StoredWorkflowContext
from minions._internal._framework.metrics_constants import (
    LABEL_ERROR_TYPE,
    LABEL_ORCHESTRATION_ID,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_PIPELINE,
    LABEL_RESOURCE,
    LABEL_RESOURCE_METHOD,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    PIPELINE_ERROR_TOTAL,
    RESOURCE_ERROR_TOTAL,
)
from tests.assets.crash.boom import BOOM_MESSAGE
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.crash.support.state_store_boom_get_contexts_for_orchestration import (
    BoomGetContextsForOrchestrationStateStore,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


GOOD_MINION = "tests.assets.crash.minions.good"
GOOD_PIPELINE = "tests.assets.crash.pipelines.emit_1_then_block"


def orchestration_id(minion_modpath: str, pipeline_modpath: str, config: str = "") -> str:
    return Gru._make_orchestration_id(
        minion_id=minion_modpath,
        minion_config_id=config,
        pipeline_id=pipeline_modpath,
    )


async def assert_gru_can_start_and_stop_known_good_minion(gru: Gru) -> None:
    result = await gru.start_orchestration(GOOD_PIPELINE, GOOD_MINION)
    assert result.success
    assert result.orchestration_id is not None
    assert isinstance(gru._logger, InMemoryLogger)
    assert await gru._logger.wait_for_log("Workflow succeeded", timeout=1.0, poll_interval=0.01)
    stop = await gru.stop_orchestration(result.orchestration_id)
    assert stop.success
    assert gru._runtime_state_snapshot() == {}


def assert_counter(metrics: InMemoryMetrics, metric_name: str, labels: dict[str, str]) -> None:
    sample = InMemoryMetrics.find_sample(metrics.snapshot_counters()[metric_name], labels)
    assert sample["value"] >= 1


@pytest.mark.asyncio
async def test_start_orchestration_contains_state_store_resume_read_failure(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
) -> None:
    state_store = BoomGetContextsForOrchestrationStateStore(logger=logger)

    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(GOOD_PIPELINE, GOOD_MINION)

        assert not result.success
        assert result.reason == "GoodMinion.startup failed (tests/assets/crash/minions/good.py)"
        assert logger.has_log(
            "BoomGetContextsForOrchestrationStateStore.get_contexts_for_orchestration failed",
            log_kwargs={"error_type": "BoomError"},
        )
        assert logger.has_log("Failed to start orchestration", log_kwargs={"error_message": BOOM_MESSAGE})
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_start_orchestration_fails_closed_on_persisted_workflow_decode_mismatch(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    expected_orchestration_id = orchestration_id(GOOD_MINION, GOOD_PIPELINE)
    persisted_context = MinionWorkflowContext(
        orchestration_id=expected_orchestration_id,
        minion_modpath=GOOD_MINION,
        workflow_id="wf-incompatible",
        event=SimpleEvent(timestamp=1.0),
        context=SimpleContext(price=1.23),
        context_cls=SimpleContext,
        next_step_index=0,
    )
    state_store._contexts["wf-incompatible"] = StoredWorkflowContext(
        workflow_id="wf-incompatible",
        orchestration_id=expected_orchestration_id,
        context=serialize_persisted_workflow_context(persisted_context),
    )

    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(GOOD_PIPELINE, GOOD_MINION)

        assert not result.success
        assert result.reason is not None
        assert "could not be decoded with the current Minion event and workflow context types" in result.reason
        assert result.suggestion is not None
        assert "drain the orchestration" in result.suggestion
        assert (
            f"delete the persisted workflow contexts for orchestration {expected_orchestration_id!r}"
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
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("minion_modpath", "pipeline_modpath"),
    [
        ("tests.assets.crash.minions.boom_startup", GOOD_PIPELINE),
        ("tests.assets.crash.minions.boom_load_config", GOOD_PIPELINE),
        (GOOD_MINION, "tests.assets.crash.pipelines.boom_startup"),
        ("tests.assets.crash.minions.depends_on_boom_startup_resource", GOOD_PIPELINE),
    ],
)
async def test_start_orchestration_contains_user_code_startup_failures(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    minion_modpath: str,
    pipeline_modpath: str,
    tests_dir: Path,
) -> None:
    config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
            minion_config_path=config_path if "boom_load_config" in minion_modpath else None,
        )

        assert not result.success
        assert logger.has_log("Failed to start orchestration", log_kwargs={"error_message": BOOM_MESSAGE})
        assert gru._runtime_state_snapshot() == {}
        await assert_gru_can_start_and_stop_known_good_minion(gru)


@pytest.mark.asyncio
async def test_minion_step_failure_is_logged_measured_and_contained(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(GOOD_PIPELINE, "tests.assets.crash.minions.boom_step")
        assert result.success

        assert await logger.wait_for_log(
            "Workflow failed",
            log_kwargs={"error_type": "BoomError"},
            timeout=1.0,
            poll_interval=0.01,
        )
        assert_counter(
            metrics,
            MINION_WORKFLOW_STEP_FAILED_TOTAL,
            {
                LABEL_ORCHESTRATION_ID: orchestration_id(
                    "tests.assets.crash.minions.boom_step",
                    "tests.assets.crash.pipelines.emit_1_then_block",
                ),
                LABEL_MINION_WORKFLOW_STEP: "step_1",
                LABEL_ERROR_TYPE: "BoomError",
            },
        )
        assert_counter(
            metrics,
            MINION_WORKFLOW_FAILED_TOTAL,
            {
                LABEL_ORCHESTRATION_ID: orchestration_id(
                    "tests.assets.crash.minions.boom_step",
                    "tests.assets.crash.pipelines.emit_1_then_block",
                ),
                LABEL_ERROR_TYPE: "BoomError",
            },
        )
        stop = await gru.stop_orchestration(result.orchestration_id or "")
        assert stop.success
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_pipeline_produce_event_failure_is_logged_measured_and_shutdown_is_clean(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration("tests.assets.crash.pipelines.boom_produce_event", GOOD_MINION)
        assert result.success

        assert await logger.wait_for_log(
            "Gru runtime task failure observed",
            log_kwargs={"component": "pipeline"},
            timeout=1.0,
            poll_interval=0.01,
        )
        assert_counter(
            metrics,
            PIPELINE_ERROR_TOTAL,
            {
                LABEL_PIPELINE: "tests.assets.crash.pipelines.boom_produce_event",
                LABEL_ERROR_TYPE: "BoomError",
            },
        )
        shutdown = await gru.shutdown()
        assert shutdown.success


@pytest.mark.asyncio
async def test_resource_method_failure_is_logged_measured_and_contained(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(GOOD_PIPELINE, "tests.assets.crash.minions.boom_resource_method")
        assert result.success

        assert await logger.wait_for_log(
            "Workflow failed",
            log_kwargs={"error_type": "BoomError"},
            timeout=1.0,
            poll_interval=0.01,
        )
        assert logger.has_log("Resource method failed", log_kwargs={"error_type": "BoomError"})
        assert_counter(
            metrics,
            RESOURCE_ERROR_TOTAL,
            {
                LABEL_RESOURCE: "BoomMethodResource",
                LABEL_RESOURCE_METHOD: "explode",
                LABEL_ERROR_TYPE: "BoomError",
            },
        )
        stop = await gru.stop_orchestration(result.orchestration_id or "")
        assert stop.success
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("minion_modpath", "pipeline_modpath"),
    [
        ("tests.assets.crash.minions.boom_shutdown", GOOD_PIPELINE),
        (GOOD_MINION, "tests.assets.crash.pipelines.blocking_boom_shutdown"),
        ("tests.assets.crash.minions.depends_on_boom_shutdown_resource", GOOD_PIPELINE),
    ],
)
async def test_shutdown_failures_are_reported_and_singleton_is_released(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    minion_modpath: str,
    pipeline_modpath: str,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(pipeline_modpath, minion_modpath)
        assert result.success
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        if minion_modpath.endswith(".boom_shutdown"):
            assert not stop.success
            assert logger.has_log("Failed to stop orchestration")
            assert gru._runtime_state_snapshot() == {}
        else:
            assert stop.success
            assert logger.has_log("shutdown failed during startup error recovery")
            assert gru._runtime_state_snapshot() == {}

    # The factory shutdown must release the global singleton even after a failed stop path.
    async with gru_factory(
        logger=InMemoryLogger(),
        metrics=InMemoryMetrics(),
        state_store=InMemoryStateStore(logger=InMemoryLogger()),
    ) as fresh_gru:
        await assert_gru_can_start_and_stop_known_good_minion(fresh_gru)
