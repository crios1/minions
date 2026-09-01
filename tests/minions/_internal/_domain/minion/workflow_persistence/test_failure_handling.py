import asyncio
import contextlib
from typing import Callable

import pytest

from minions import Minion, minion_step
from minions._internal._domain.gru import Gru
from minions._internal._framework.logger import ERROR, WARNING
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
from tests.assets.contexts.empty import EmptyContext
from tests.assets.contexts.int_value import IntValueContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
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
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="continue-on-failure",
    )

    m._mn_mark_running()
    await m._mn_accept_event(EmptyEvent())
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
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 4
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 3
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 1
    assert metrics.snapshot_histogram_count_total(MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS) == 4
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
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.1,
        workflow_persistence_retry_jitter_ratio=0.0,
    )

    m._mn_mark_running()
    await m._mn_accept_event(EmptyEvent())
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
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 4
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 3
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
        minion_id="dummy-minion-id",
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
    await m._mn_accept_event(EmptyEvent())
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
    states = await m._mn_workflow_persistence_state_snapshot()
    assert len(states) == 1
    state = next(iter(states.values()))
    assert state.persisted_next_step_index == 0
    assert state.next_step_index == 1
    assert state.risk_kind == "stale_checkpoint"
    async with m._mn_tasks_gate:
        workflow_task = next(iter(m._mn_workflow_tasks))

    workflow_task.cancel()
    await m._mn_wait_until_tasks_idle(timeout=1.0)

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
    assert await m._mn_workflow_persistence_state_snapshot() == {}
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

        rejected_stop = await gru.stop_orchestration(started.orchestration_id)

        assert not rejected_stop.success
        assert rejected_stop.blocked_by_persistence_risk
        assert len(rejected_stop.persistence_risks) == 1
        assert rejected_stop.persistence_risks[0].kind == "stale_checkpoint"
        assert step_calls == ["change_context_and_enable_save_failures"]
        assert minion._mn_workflow_tasks

        forced_stop = await gru.stop_orchestration(
            started.orchestration_id,
            force=True,
        )

        assert forced_stop.success
        assert not forced_stop.blocked_by_persistence_risk
        assert forced_stop.persistence_risks == rejected_stop.persistence_risks
        stopped_log = next(log for log in logger.logs if log.msg == "Orchestration stopped")
        assert stopped_log.kwargs["force_requested"] is True
        assert stopped_log.kwargs["persistence_risks"][0]["kind"] == "stale_checkpoint"
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
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_jitter_ratio=0.0,
    )

    m._mn_mark_running()
    await asyncio.gather(
        m._mn_accept_event(EmptyEvent()),
        m._mn_accept_event(EmptyEvent()),
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
        minion_id="dummy-minion-id",
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
    await m._mn_accept_event(EmptyEvent())
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
        minion_id="dummy-minion-id",
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
    await m._mn_accept_event(EmptyEvent())
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
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 4
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 2
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
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
    )

    m._mn_mark_running()
    await m._mn_accept_event(EmptyEvent())
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
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 2
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 1
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
