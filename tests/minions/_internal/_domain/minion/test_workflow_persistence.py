import asyncio
import contextlib
import random
from typing import Callable, Literal, NoReturn

import pytest

from minions import Minion, minion_step
from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import WorkflowPersistencePoint
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.logger import ERROR, WARNING, Logger
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POINT,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_ORCHESTRATION_ID,
    LABEL_STATE_STORE,
    MINION_WORKFLOW_INFLIGHT_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
)
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
)
from minions._internal._framework.state_store import StateStore
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.contexts.int_value import IntValueContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_noop import NoOpMinion
from tests.assets.support.pipeline_triggered import TriggeredPipeline
from tests.assets.support.state_store_failable import FailableStateStore
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import assert_runtime_empty


async def _wait_until(
    condition: Callable[[], bool],
    *,
    timeout: float = 1.0,
    poll_interval: float = 0.005,
) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(poll_interval)
    raise TimeoutError("condition did not become true before timeout")


class _GatedStateStore(InMemoryStateStore):
    """Allow a test to pause exactly one selected save or delete attempt until released."""

    def __init__(
        self,
        logger: Logger,
        *,
        gated_operation: Literal["save", "delete"],
        fail_after_release: bool,
    ) -> None:
        super().__init__(logger)
        self._gated_operation = gated_operation
        self._fail_after_release = fail_after_release
        self.attempt_count = 0
        self.attempt_started = asyncio.Event()
        self._attempt_release = asyncio.Event()

    def release_attempt(self) -> None:
        concurrent_attempt_count = self.attempt_count
        try:
            if concurrent_attempt_count != 1:
                raise AssertionError(
                    "expected exactly one concurrent persistence attempt, "
                    f"got {concurrent_attempt_count}"
                )
        finally:
            self._attempt_release.set()

    async def _gate(self, operation: Literal["save", "delete"]) -> None:
        if operation != self._gated_operation:
            return
        self.attempt_count += 1
        self.attempt_started.set()
        await self._attempt_release.wait()
        if self._fail_after_release:
            raise RuntimeError(f"controlled {operation} failure")

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        await self._gate("save")
        await super().save_context(workflow_id, orchestration_id, context)

    async def delete_context(self, workflow_id: str) -> None:
        await self._gate("delete")
        await super().delete_context(workflow_id)


def _make_no_op_minion(
    *,
    store: StateStore,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    delay_seconds: float,
    max_delay_seconds: float,
    backoff_multiplier: float,
    jitter_ratio: float,
):
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
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=delay_seconds,
        workflow_persistence_retry_max_delay_seconds=max_delay_seconds,
        workflow_persistence_retry_backoff_multiplier=backoff_multiplier,
        workflow_persistence_retry_jitter_ratio=jitter_ratio,
    )


def _make_empty_workflow_context(orchestration_id: str):
    return MinionWorkflowContext(
        orchestration_id=orchestration_id,
        workflow_id="dummy-workflow-id",
        event=EmptyEvent(),
        context=EmptyContext(),
    )


@pytest.mark.parametrize(
    ("persistence_point", "step_name"),
    [
        ("workflow_start", None),
        ("before_step", "step"),
        ("workflow_resolve", None),
    ],
)
def test_persistence_points_accept_valid_step_name_combinations(
    persistence_point: WorkflowPersistencePoint,
    step_name: str | None,
):
    Minion._mn_validate_workflow_persistence_point(
        persistence_point,
        step_name,
    )


@pytest.mark.parametrize(
    ("persistence_point", "step_name", "message"),
    [
        (
            "before_step",
            None,
            "step_name is required for the 'before_step' persistence point",
        ),
        (
            "workflow_start",
            "step",
            "step_name is only valid for the 'before_step' persistence point",
        ),
    ],
)
def test_persistence_points_reject_incompatible_step_names(
    persistence_point: WorkflowPersistencePoint,
    step_name: str | None,
    message: str,
):
    with pytest.raises(ValueError, match=message):
        Minion._mn_validate_workflow_persistence_point(
            persistence_point,
            step_name,
        )


def test_persistence_points_reject_unknown_values():
    with pytest.raises(ValueError, match="persistence_point must be one of"):
        Minion._mn_validate_workflow_persistence_point(
            "unknown",  # pyright: ignore[reportArgumentType]
            None,
        )


@pytest.mark.asyncio
async def test_continue_on_failure_policy_advances_after_save_failure_and_persists_next_checkpoint(  # noqa: E501
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []

    class TransientSaveFailureMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def enable_save_failures(self):
            step_calls.append("enable_save_failures")
            store.save_failures.enable()

        @minion_step
        async def disable_save_failures(self):
            step_calls.append("disable_save_failures")
            store.save_failures.disable()

        @minion_step
        async def complete_workflow(self):
            step_calls.append("complete_workflow")

    store = FailableStateStore(logger=logger)
    m = TransientSaveFailureMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.continue_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="continue-on-failure",
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == [
        "enable_save_failures",
        "disable_save_failures",
        "complete_workflow",
    ]
    assert store.save_failures.count == 1
    assert any(
        deserialize_workflow_context_blob(stored.context).next_step_index == 2
        for stored in store.saved_context_history
    )
    assert logger.has_log("Workflow continuing after persistence failure")
    failure_log = next(
        log for log in logger.logs if log.msg == "Workflow continuing after persistence failure"
    )
    assert failure_log.kwargs["persistence_point"] == "before_step"
    assert failure_log.kwargs["step_name"] == "disable_save_failures"
    assert "checkpoint" not in failure_log.kwargs
    assert failure_log.kwargs["persistence_failure_stage"] == "save"
    assert failure_log.kwargs["persistence_retryable"] is True
    assert (
        failure_log.kwargs["suggestion"]
        == "Ensure the configured StateStore is available and can persist workflow context blobs."
    )
    assert failure_log.kwargs["error_type"] == "RuntimeError"
    assert failure_log.kwargs[LABEL_STATE_STORE] == "FailableStateStore"
    assert failure_log.kwargs["event_type"] == "EmptyEvent"
    assert failure_log.kwargs["context_type"] == "EmptyContext"
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 5
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 4
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 1
    assert metrics.snapshot_histogram_count_total(MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS) == 5
    assert MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE not in metrics.snapshot_gauges()


@pytest.mark.asyncio
async def test_idle_until_persisted_policy_idles_workflow_until_save_retry_succeeds(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []
    workflow_continued = asyncio.Event()

    class TransientSaveFailureMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def enable_save_failures(self):
            step_calls.append("enable_save_failures")
            store.save_failures.enable()

        @minion_step
        async def continue_after_persistence(self):
            step_calls.append("continue_after_persistence")
            workflow_continued.set()

    store = FailableStateStore(logger=logger)
    m = TransientSaveFailureMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.idle_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.1,
        workflow_persistence_retry_jitter_ratio=0.0,
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await store.save_failures.wait_for(1)

    assert step_calls == ["enable_save_failures"]
    assert not workflow_continued.is_set()

    store.save_failures.disable()
    await asyncio.wait_for(workflow_continued.wait(), timeout=1.0)
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["enable_save_failures", "continue_after_persistence"]
    assert store.save_failures.count == 1
    assert logger.has_log("Workflow idled waiting for persistence")
    assert logger.has_log("Workflow persistence resumed")
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 5
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 4
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 1
    blocked_value = metrics.snapshot_gauge_value(
        MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "FailableStateStore",
        },
    )
    assert blocked_value == 0


@pytest.mark.asyncio
async def test_retry_delays_follow_exponential_backoff_and_remain_capped(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    monkeypatch: pytest.MonkeyPatch,
):
    store = FailableStateStore(logger=logger)
    store.save_failures.enable()
    m = _make_no_op_minion(
        store=store,
        logger=logger,
        metrics=metrics,
        delay_seconds=0.01,
        max_delay_seconds=0.04,
        backoff_multiplier=2.0,
        jitter_ratio=0.0,
    )
    ctx = _make_empty_workflow_context(m._mn_orchestration_id)
    sleep_delays: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(sleep_delays) == 4:
            store.save_failures.disable()

    def fail_if_jitter_is_applied(_lower: float, _upper: float) -> float:
        raise AssertionError("zero jitter must not sample randomness")

    monkeypatch.setattr(asyncio, "sleep", record_sleep)
    monkeypatch.setattr(random, "uniform", fail_if_jitter_is_applied)

    persisted = await m._mn_run_workflow_persistence_operation(
        ctx,
        persistence_point="workflow_start",
    )

    assert persisted
    assert sleep_delays == [0.01, 0.02, 0.04, 0.04]
    assert store.save_failures.count == 4
    idle_log = next(
        log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"
    )
    assert idle_log.kwargs["persistence_point"] == "workflow_start"
    assert "step_name" not in idle_log.kwargs
    assert "checkpoint" not in idle_log.kwargs


@pytest.mark.asyncio
async def test_retry_jitter_respects_configured_bounds(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    monkeypatch: pytest.MonkeyPatch,
):
    store = FailableStateStore(logger=logger)
    store.save_failures.enable()
    m = _make_no_op_minion(
        store=store,
        logger=logger,
        metrics=metrics,
        delay_seconds=1.0,
        max_delay_seconds=1.0,
        backoff_multiplier=1.0,
        jitter_ratio=0.25,
    )
    ctx = _make_empty_workflow_context(m._mn_orchestration_id)
    sleep_delays: list[float] = []
    sampled_bounds: list[tuple[float, float]] = []

    async def record_sleep(delay: float) -> None:
        sleep_delays.append(delay)
        if len(sleep_delays) == 2:
            store.save_failures.disable()

    def sample_each_bound(lower: float, upper: float) -> float:
        sampled_bounds.append((lower, upper))
        return lower if len(sampled_bounds) == 1 else upper

    monkeypatch.setattr(asyncio, "sleep", record_sleep)
    monkeypatch.setattr(random, "uniform", sample_each_bound)

    persisted = await m._mn_run_workflow_persistence_operation(
        ctx,
        persistence_point="workflow_start",
    )

    assert persisted
    assert sampled_bounds == [(-0.25, 0.25), (-0.25, 0.25)]
    assert sleep_delays == [0.75, 1.25]
    assert store.save_failures.count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "fail"),
    [
        pytest.param("save", False, id="save-succeeds"),
        pytest.param("save", True, id="save-fails"),
        pytest.param("delete", False, id="delete-succeeds"),
        pytest.param("delete", True, id="delete-fails"),
    ],
)
async def test_cancellation_propagates_after_one_active_attempt_finishes(
    operation: Literal["save", "delete"],
    fail: bool,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    store = _GatedStateStore(
        logger,
        gated_operation=operation,
        fail_after_release=fail,
    )
    m = _make_no_op_minion(
        store=store,
        logger=logger,
        metrics=metrics,
        delay_seconds=0.01,
        max_delay_seconds=0.01,
        backoff_multiplier=1.0,
        jitter_ratio=0.0,
    )
    ctx = _make_empty_workflow_context(m._mn_orchestration_id)
    if operation == "delete":
        await store._mn_serialize_and_save_context(ctx)

    persistence_task = asyncio.create_task(
        m._mn_run_workflow_persistence_operation(
            ctx,
            persistence_point=(
                "workflow_start" if operation == "save" else "workflow_resolve"
            ),
        )
    )
    await asyncio.wait_for(store.attempt_started.wait(), timeout=1.0)

    persistence_task.cancel()
    await asyncio.sleep(0)

    assert not persistence_task.done()

    store.release_attempt()
    with pytest.raises(asyncio.CancelledError):
        await persistence_task

    persisted_contexts = await store.get_all_contexts()
    expected_context_remains = (operation == "save" and not fail) or (
        operation == "delete" and fail
    )
    assert bool(persisted_contexts) is expected_context_remains
    assert store.attempt_count == 1
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 1
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == (
        0 if fail else 1
    )
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == (
        1 if fail else 0
    )
    assert MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE not in metrics.snapshot_gauges()


@pytest.mark.asyncio
async def test_unexpected_active_attempt_error_is_logged_without_replacing_cancellation(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    monkeypatch: pytest.MonkeyPatch,
):
    store = NoOpStateStore()
    m = _make_no_op_minion(
        store=store,
        logger=logger,
        metrics=metrics,
        delay_seconds=0.01,
        max_delay_seconds=0.01,
        backoff_multiplier=1.0,
        jitter_ratio=0.0,
    )
    ctx = _make_empty_workflow_context(m._mn_orchestration_id)
    attempt_count = 0
    attempt_started = asyncio.Event()
    release_attempt = asyncio.Event()

    async def raise_attempt_error(_ctx: object, **_kwargs: object) -> NoReturn:
        nonlocal attempt_count
        attempt_count += 1
        attempt_started.set()
        await release_attempt.wait()
        raise RuntimeError("controlled attempt error")

    monkeypatch.setattr(
        m,
        "_mn_run_workflow_persistence_attempt",
        raise_attempt_error,
    )
    persistence_task = asyncio.create_task(
        m._mn_run_workflow_persistence_operation(
            ctx,
            persistence_point="workflow_start",
        )
    )
    await asyncio.wait_for(attempt_started.wait(), timeout=1.0)

    persistence_task.cancel()
    await asyncio.sleep(0)
    release_attempt.set()

    with pytest.raises(asyncio.CancelledError):
        await persistence_task

    assert attempt_count == 1
    failure_log = next(
        log
        for log in logger.logs
        if log.msg == "Workflow persistence attempt failed during cancellation"
    )
    assert failure_log.kwargs["error_type"] == "RuntimeError"
    assert failure_log.kwargs["error_message"] == "controlled attempt error"
    assert failure_log.kwargs["persistence_point"] == "workflow_start"
    assert failure_log.kwargs["persistence_operation"] == "save"


@pytest.mark.asyncio
async def test_workflow_cancellation_during_retry_wait_preserves_checkpoint_and_clears_tracking(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []

    class SaveFailureBeforeNextStepMinion(Minion[EmptyEvent, IntValueContext]):
        @minion_step
        async def change_context_and_enable_save_failures(self):
            step_calls.append("change_context_and_enable_save_failures")
            self.context.value = 1
            state_store = self._mn_state_store
            assert isinstance(state_store, FailableStateStore)
            state_store.save_failures.enable()

        @minion_step
        async def must_not_run(self):
            step_calls.append("must_not_run")

    store = FailableStateStore(logger=logger)
    m = SaveFailureBeforeNextStepMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.save_failure_before_next_step_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=60.0,
        workflow_persistence_retry_max_delay_seconds=60.0,
        workflow_persistence_retry_jitter_ratio=0.0,
    )
    blocked_labels = {
        LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
        LABEL_MINION: m._mn_minion_id,
        LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: "before_step",
        LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
        LABEL_STATE_STORE: "FailableStateStore",
    }

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await store.save_failures.wait_for(1)
    await _wait_until(
        lambda: (
            MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE in metrics.snapshot_gauges()
            and metrics.snapshot_gauge_value(
                MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
                blocked_labels,
            )
            == 1
        )
    )
    async with m._mn_tasks_gate:
        workflow_task = next(iter(m._mn_workflow_tasks))

    workflow_task.cancel()
    await m._mn_wait_until_all_tasks_idle(timeout=1.0)

    persisted_contexts = await store.get_all_contexts()
    assert len(persisted_contexts) == 1
    persisted = deserialize_workflow_context_blob(persisted_contexts[0].context)
    assert persisted.next_step_index == 0
    assert persisted.context.value == 0
    assert step_calls == ["change_context_and_enable_save_failures"]
    assert store.save_failures.count == 1
    assert workflow_task.cancelled()
    assert not m._mn_workflow_tasks
    assert not m._mn_service_tasks
    assert not m._mn_workflow_persistence_blocked_counts
    assert metrics.snapshot_gauge_value(
        MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
        blocked_labels,
    ) == 0
    assert metrics.snapshot_gauge_value(
        MINION_WORKFLOW_INFLIGHT_GAUGE,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
        },
    ) == 0


@pytest.mark.asyncio
async def test_stopping_orchestration_during_retry_wait_preserves_unfinished_workflow_and_cleans_up(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
):
    step_calls: list[str] = []

    class EmptyEventPipeline(TriggeredPipeline[EmptyEvent]):
        async def produce_event(self):
            return EmptyEvent()

    class SaveFailureBeforeNextStepMinion(Minion[EmptyEvent, IntValueContext]):
        @minion_step
        async def change_context_and_enable_save_failures(self):
            step_calls.append("change_context_and_enable_save_failures")
            self.context.value = 1
            state_store = self._mn_state_store
            assert isinstance(state_store, FailableStateStore)
            state_store.save_failures.enable()

        @minion_step
        async def must_not_run(self):
            step_calls.append("must_not_run")

    store = FailableStateStore(logger=logger)

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=store,
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=60.0,
        workflow_persistence_retry_max_delay_seconds=60.0,
        workflow_persistence_retry_jitter_ratio=0.0,
    ) as gru:
        started = await gru.start_orchestration(
            EmptyEventPipeline,
            SaveFailureBeforeNextStepMinion,
        )
        assert started.success
        assert started.orchestration_id is not None
        orchestration = gru._orchestrations[started.orchestration_id]
        minion = orchestration.minion
        pipeline = orchestration.pipeline
        assert isinstance(pipeline, EmptyEventPipeline)
        blocked_labels = {
            LABEL_ORCHESTRATION_ID: minion._mn_orchestration_id,
            LABEL_MINION: minion._mn_minion_id,
            LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "FailableStateStore",
        }

        await pipeline.trigger_event()
        await store.save_failures.wait_for(1)
        await _wait_until(
            lambda: (
                MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE in metrics.snapshot_gauges()
                and metrics.snapshot_gauge_value(
                    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
                    blocked_labels,
                )
                == 1
            )
        )

        stopped = await gru.stop_orchestration(started.orchestration_id)

        assert stopped.success
        persisted_contexts = await store.get_all_contexts()
        assert len(persisted_contexts) == 1
        persisted = deserialize_workflow_context_blob(persisted_contexts[0].context)
        assert persisted.next_step_index == 0
        assert persisted.context.value == 0
        assert step_calls == ["change_context_and_enable_save_failures"]
        assert store.save_failures.count == 1
        assert not minion._mn_workflow_tasks
        assert not minion._mn_service_tasks
        assert not minion._mn_workflow_persistence_blocked_counts
        assert metrics.snapshot_gauge_value(
            MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
            blocked_labels,
        ) == 0
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_persistence_blocked_gauge_tracks_concurrent_workflows_for_same_label_set(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []
    synchronized_workflow_count = 0
    both_workflows_synchronized = asyncio.Event()

    class ConcurrentSaveFailureMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def synchronize_before_save_failure(self):
            nonlocal synchronized_workflow_count
            step_calls.append("synchronize_before_save_failure")
            synchronized_workflow_count += 1
            if synchronized_workflow_count == 2:
                store.save_failures.enable()
                both_workflows_synchronized.set()
            await both_workflows_synchronized.wait()

        @minion_step
        async def continue_after_persistence(self):
            step_calls.append("continue_after_persistence")

    store = FailableStateStore(logger=logger)
    m = ConcurrentSaveFailureMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.concurrent_idle_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_jitter_ratio=0.0,
    )

    m._mn_mark_running()
    await asyncio.gather(
        m._mn_handle_event(EmptyEvent()),
        m._mn_handle_event(EmptyEvent()),
    )
    await store.save_failures.wait_for(2)

    labels = {
        LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
        LABEL_MINION: m._mn_minion_id,
        LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: "before_step",
        LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
        LABEL_STATE_STORE: "FailableStateStore",
    }
    await _wait_until(
        lambda: (
            MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE in metrics.snapshot_gauges()
            and metrics.snapshot_gauge_value(
                MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
                labels,
            )
            == 2
        )
    )
    assert metrics.snapshot_gauge_value(MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE, labels) == 2
    assert step_calls == [
        "synchronize_before_save_failure",
        "synchronize_before_save_failure",
    ]

    store.save_failures.disable()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls.count("continue_after_persistence") == 2
    assert metrics.snapshot_gauge_value(MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE, labels) == 0


@pytest.mark.asyncio
async def test_idle_until_persisted_policy_reports_retry_progress_and_escalates_logs_during_sustained_save_failure(  # noqa: E501
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    save_failures_enabled = asyncio.Event()
    workflow_continued = asyncio.Event()

    class SustainedSaveFailureMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def enable_save_failures(self):
            store.save_failures.enable()
            save_failures_enabled.set()

        @minion_step
        async def continue_after_persistence(self):
            workflow_continued.set()

    store = FailableStateStore(logger=logger)
    m = SustainedSaveFailureMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.sustained_idle_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_max_delay_seconds=0.04,
        workflow_persistence_retry_backoff_multiplier=2.0,
        workflow_persistence_retry_jitter_ratio=0.0,
        workflow_persistence_retry_warning_interval_seconds=0.02,
        workflow_persistence_retry_error_after_seconds=0.03,
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(save_failures_enabled.wait(), timeout=1.0)
    assert not workflow_continued.is_set()

    await _wait_until(
        lambda: (
            len([log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"])
            >= 3
        ),
        timeout=1.0,
    )
    idle_logs = [log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"]
    initial_idle_log = idle_logs[0]
    latest_idle_log = idle_logs[-1]

    assert initial_idle_log.level == WARNING
    assert latest_idle_log.level == ERROR
    assert (
        latest_idle_log.kwargs["persistence_retry_attempts"]
        > initial_idle_log.kwargs["persistence_retry_attempts"]
    )
    assert initial_idle_log.kwargs["persistence_retry_delay_seconds"] == 0.01
    assert latest_idle_log.kwargs["persistence_retry_delay_seconds"] == 0.04

    store.save_failures.disable()
    await asyncio.wait_for(workflow_continued.wait(), timeout=1.0)
    await m._mn_wait_until_workflows_idle(timeout=2)


@pytest.mark.asyncio
async def test_workflow_success_is_delayed_until_checkpoint_delete_succeeds(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_1_done = asyncio.Event()

    class DeleteBlockingSuccessMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            step_1_done.set()

    store = FailableStateStore(logger=logger)
    store.delete_failures.enable()
    m = DeleteBlockingSuccessMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.delete_blocking_success_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="continue-on-failure",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_max_delay_seconds=0.02,
        workflow_persistence_retry_backoff_multiplier=1.0,
        workflow_persistence_retry_jitter_ratio=0.0,
        workflow_persistence_retry_warning_interval_seconds=0.01,
        workflow_persistence_retry_error_after_seconds=None,
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(step_1_done.wait(), timeout=1.0)
    await store.delete_failures.wait_for(2)

    blocked_labels = {
        LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
        LABEL_MINION: m._mn_minion_id,
        LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: "workflow_resolve",
        LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "delete",
        LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "delete",
        LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "continue-on-failure",
        LABEL_STATE_STORE: "FailableStateStore",
    }
    await _wait_until(
        lambda: (
            MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE in metrics.snapshot_gauges()
            and metrics.snapshot_gauge_value(
                MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
                blocked_labels,
            )
            == 1
        )
    )
    assert not logger.has_log("Workflow succeeded")
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_SUCCEEDED_TOTAL) == 0
    blocked_value = metrics.snapshot_gauge_value(
        MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
        blocked_labels,
    )
    assert blocked_value == 1

    store.delete_failures.disable()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert logger.has_log("Workflow idled waiting for checkpoint delete")
    assert logger.has_log("Workflow checkpoint delete resumed")
    delete_idle_log = next(
        log for log in logger.logs if log.msg == "Workflow idled waiting for checkpoint delete"
    )
    assert delete_idle_log.kwargs["persistence_point"] == "workflow_resolve"
    assert "step_name" not in delete_idle_log.kwargs
    assert "checkpoint" not in delete_idle_log.kwargs
    assert logger.has_log("Workflow succeeded")
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_SUCCEEDED_TOTAL) == 1
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 5
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 3
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 2
    blocked_value = metrics.snapshot_gauge_value(
        MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
        blocked_labels,
    )
    assert blocked_value == 0


@pytest.mark.asyncio
async def test_serialization_failure_is_non_retryable_and_preserves_prior_checkpoint(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    step_calls: list[str] = []

    class UnserializableValue:
        pass

    class UnserializableContextMinion(Minion[EmptyEvent, IntValueContext]):
        @minion_step
        async def make_context_unserializable(self):
            step_calls.append("make_context_unserializable")
            self.context.value = UnserializableValue()  # pyright: ignore[reportAttributeAccessIssue]

        @minion_step
        async def continue_workflow(self):
            step_calls.append("continue_workflow")

    m = UnserializableContextMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=state_store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.non_retryable_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["make_context_unserializable"]
    persisted_contexts = await state_store.get_all_contexts()
    assert len(persisted_contexts) == 1
    decoded = deserialize_workflow_context_blob(persisted_contexts[0].context)
    assert decoded.next_step_index == 0
    assert decoded.context.value == 0
    failure_log = next(
        log
        for log in logger.logs
        if log.msg == "Workflow persistence failed with non-retryable error"
    )
    assert failure_log.kwargs["persistence_failure_stage"] == "serialize"
    assert failure_log.kwargs["persistence_retryable"] is False
    assert failure_log.kwargs["persistence_retry_delay_seconds"] is None
    assert failure_log.kwargs["suggestion"] == (
        "Ensure workflow event and context values are supported by the Minions persistence codec."
    )
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 3
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 2
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 1
    failure_value = metrics.snapshot_counter_value(
        MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_MINION_WORKFLOW_PERSISTENCE_POINT: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "serialize",
            LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE: "false",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "InMemoryStateStore",
        },
    )
    assert failure_value == 1
