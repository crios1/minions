import asyncio
from typing import Callable

import pytest

from minions import Minion, minion_step
from minions._internal._framework.logger import ERROR
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_ORCHESTRATION_ID,
    LABEL_STATE_STORE,
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
from tests.assets.support.state_store_failable import FailableStateStore
from tests.assets.support.state_store_inmemory import InMemoryStateStore


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
async def test_workflow_persistence_continue_on_failure_advances_and_retries_at_next_checkpoint(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []

    class ContinueOnFailureMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            step_calls.append("step_1")
            store.save_failures.enable()

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")
            store.save_failures.disable()

        @minion_step
        async def step_3(self):
            step_calls.append("step_3")

    store = FailableStateStore(logger=logger)
    m = ContinueOnFailureMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path="dummy-config-path",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.continue_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="continue-on-failure",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1", "step_2", "step_3"]
    assert store.save_failures.count == 1
    assert any(
        deserialize_workflow_context_blob(stored.context).next_step_index == 2
        for stored in store.saved_context_history
    )
    assert logger.has_log("Workflow continuing after persistence failure")
    failure_log = next(
        log for log in logger.logs if log.msg == "Workflow continuing after persistence failure"
    )
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
async def test_workflow_persistence_idle_until_persisted_blocks_next_step_until_retry_succeeds(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []
    step_1_done = asyncio.Event()
    step_2_started = asyncio.Event()

    class IdleUntilPersistedMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            step_calls.append("step_1")
            store.save_failures.enable()
            step_1_done.set()

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")
            step_2_started.set()

    store = FailableStateStore(logger=logger)
    m = IdleUntilPersistedMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path="dummy-config-path",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.idle_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(step_1_done.wait(), timeout=1.0)
    await store.save_failures.wait_for(2)

    assert step_calls == ["step_1"]
    assert not step_2_started.is_set()

    store.save_failures.disable()
    await asyncio.wait_for(step_2_started.wait(), timeout=1.0)
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1", "step_2"]
    assert store.save_failures.count == 2
    assert logger.has_log("Workflow idled waiting for persistence")
    assert logger.has_log("Workflow persistence resumed")
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 6
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 4
    assert metrics.snapshot_counter_value_total(MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 2
    blocked_value = metrics.snapshot_gauge_value(
        MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "FailableStateStore",
        },
    )
    assert blocked_value == 0


@pytest.mark.asyncio
async def test_workflow_persistence_blocked_gauge_counts_concurrent_workflows_for_same_label_set(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_calls: list[str] = []
    step_1_count = 0
    both_workflows_reached_step_1 = asyncio.Event()

    class ConcurrentIdleUntilPersistedMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            nonlocal step_1_count
            step_calls.append("step_1")
            step_1_count += 1
            if step_1_count == 2:
                store.save_failures.enable()
                both_workflows_reached_step_1.set()
            await both_workflows_reached_step_1.wait()

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")

    store = FailableStateStore(logger=logger)
    m = ConcurrentIdleUntilPersistedMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path="dummy-config-path",
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
        LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "before_step",
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
    assert step_calls == ["step_1", "step_1"]

    store.save_failures.disable()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls.count("step_2") == 2
    assert metrics.snapshot_gauge_value(MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE, labels) == 0


@pytest.mark.asyncio
async def test_workflow_persistence_idle_until_persisted_relogs_and_escalates_sustained_failure(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    step_1_done = asyncio.Event()
    step_2_started = asyncio.Event()

    class SustainedIdleMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            store.save_failures.enable()
            step_1_done.set()

        @minion_step
        async def step_2(self):
            step_2_started.set()

    store = FailableStateStore(logger=logger)
    m = SustainedIdleMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path="dummy-config-path",
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
    await asyncio.wait_for(step_1_done.wait(), timeout=1.0)
    assert not step_2_started.is_set()

    await _wait_until(
        lambda: (
            len([log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"])
            >= 3
        ),
        timeout=1.0,
    )
    idle_logs = [log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"]
    assert any(log.level == ERROR for log in idle_logs)
    assert (
        idle_logs[-1].kwargs["persistence_retry_attempts"]
        > idle_logs[0].kwargs["persistence_retry_attempts"]
    )
    assert {log.kwargs["persistence_retry_delay_seconds"] for log in idle_logs} <= {
        0.01,
        0.02,
        0.04,
    }

    store.save_failures.disable()
    await asyncio.wait_for(step_2_started.wait(), timeout=1.0)
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
        config_path="dummy-config-path",
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
        LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "workflow_resolve",
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
async def test_workflow_persistence_serialization_failure_is_non_retryable_and_preserves_prior_checkpoint(  # noqa: E501
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    step_calls: list[str] = []

    class UnserializableValue:
        pass

    class MyMinion(Minion[EmptyEvent, IntValueContext]):
        @minion_step
        async def step_1(self):
            step_calls.append("step_1")
            self.context.value = UnserializableValue()  # pyright: ignore[reportAttributeAccessIssue]

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")

    m = MyMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path="dummy-config-path",
        state_store=state_store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.non_retryable_persistence_minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1"]
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
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "serialize",
            LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE: "false",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "InMemoryStateStore",
        },
    )
    assert failure_value == 1
