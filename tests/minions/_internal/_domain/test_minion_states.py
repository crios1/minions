import asyncio
from typing import Any, Callable

import msgspec
import pytest

from minions import Minion, minion_step
from minions._internal._domain.exceptions import AbortWorkflow
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.logger import ERROR
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_ORCHESTRATION_ID,
    LABEL_STATE_STORE,
    LABEL_STATUS,
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
)
from minions._internal._framework.minion_workflow_context_codec import (
    PersistedMinionWorkflowContext,
    WorkflowContextTypeMismatchError,
    deserialize_workflow_context_blob,
    serialize_persisted_workflow_context,
)
from minions._internal._framework.state_store import StoredWorkflowContext
from minions._internal._utils.serialization import serialize
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.events.int_value import IntValueEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_failable import FailableStateStore
from tests.assets.support.state_store_inmemory import InMemoryStateStore


class NoOpMinion(Minion[EmptyEvent, EmptyContext]):
    @minion_step
    async def step_1(self):
        return


def _make_no_op_minion(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> NoOpMinion:
    return NoOpMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_id="dummy-minion-id",
        minion_config_id="dummy-minion-config-id",
        pipeline_id="dummy-pipeline-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=state_store,
        metrics=metrics,
        logger=logger,
    )


async def _add_pending_service_task(
    minion: Minion[Any, Any],
    event: asyncio.Event,
) -> asyncio.Task[None]:
    async def _wait_for_event() -> None:
        await event.wait()

    task = asyncio.create_task(_wait_for_event())
    async with minion._mn_tasks_gate:
        minion._mn_service_tasks.add(task)

    task.add_done_callback(lambda t: minion._mn_service_tasks.discard(t))

    return task


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
async def test_wait_until_tasks_idle_ignores_service_tasks_by_default(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    minion = _make_no_op_minion(logger, metrics, state_store)

    event = asyncio.Event()
    task = await _add_pending_service_task(minion, event)

    try:
        await minion._mn_wait_until_tasks_idle(
            timeout=0.01,
            timeout_msg="NoOpMinion tasks did not become idle within 0.01s",
        )
        assert not task.done()
    finally:
        event.set()
        await task


@pytest.mark.asyncio
async def test_wait_until_tasks_idle_can_include_service_tasks(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    minion = _make_no_op_minion(logger, metrics, state_store)

    event = asyncio.Event()
    task = await _add_pending_service_task(minion, event)

    try:
        with pytest.raises(
            TimeoutError,
            match="NoOpMinion tasks did not become idle within 0.01s",
        ):
            await minion._mn_wait_until_tasks_idle(
                timeout=0.01,
                include_service_tasks=True,
                timeout_msg="NoOpMinion tasks did not become idle within 0.01s",
            )
    finally:
        event.set()
        await task


@pytest.mark.asyncio
async def test_workflow_aborted_increments_aborted_counter(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    # define a minion whose first step aborts
    class AbortMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            raise AbortWorkflow()

    # start the minion by calling _mn_handle_event directly with a dummy event
    m = AbortMinion(
        "iid",
        "ck",
        "tests.assets.abort_minion",
        "cfg",
        state_store,
        metrics,
        logger,
        minion_id="tests.assets.abort_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
    )
    m._mn_started.set()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    # wait until the state store has recorded deletion (workflow finished)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    counters = metrics.snapshot().get("counter", {})
    aborted_total = sum(s.get("value", 0) for s in counters.get(MINION_WORKFLOW_ABORTED_TOTAL, []))

    # workflow started should be incremented and aborted counter incremented
    assert counters.get(MINION_WORKFLOW_STARTED_TOTAL)
    assert aborted_total == 1


@pytest.mark.asyncio
async def test_workflow_failed_increments_failed_counter(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class FailMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            raise RuntimeError("boom")

    m = FailMinion(
        "iid",
        "ck",
        "tests.assets.fail_minion",
        "cfg",
        state_store,
        metrics,
        logger,
        minion_id="tests.assets.fail_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
    )
    m._mn_started.set()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    counters = metrics.snapshot().get("counter", {})
    failed_total = sum(s.get("value", 0) for s in counters.get(MINION_WORKFLOW_FAILED_TOTAL, []))

    assert counters.get(MINION_WORKFLOW_STARTED_TOTAL)
    assert failed_total == 1
    workflow_duration_count = metrics.snapshot_histogram_count(
        MINION_WORKFLOW_DURATION_SECONDS,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_STATUS: "failed",
        },
    )
    step_duration_count = metrics.snapshot_histogram_count(
        MINION_WORKFLOW_STEP_DURATION_SECONDS,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_MINION_WORKFLOW_STEP: "step_1",
            LABEL_STATUS: "failed",
        },
    )
    assert workflow_duration_count == 1
    assert step_duration_count == 1


@pytest.mark.asyncio
async def test_workflow_cancellation_records_interrupted_duration_status_and_keeps_context(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    step_started = asyncio.Event()
    step_can_finish = asyncio.Event()

    class InterruptedMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            step_started.set()
            await step_can_finish.wait()

    m = InterruptedMinion(
        "iid",
        "ck",
        "tests.assets.interrupted_minion",
        "cfg",
        state_store,
        metrics,
        logger,
        minion_id="tests.assets.interrupted_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
    )
    m._mn_started.set()
    await m._mn_handle_event(EmptyEvent())
    await asyncio.wait_for(step_started.wait(), timeout=1.0)
    async with m._mn_tasks_gate:
        workflow_tasks = list(m._mn_workflow_tasks)
    assert len(workflow_tasks) == 1
    workflow_tasks[0].cancel()
    await m._mn_wait_until_workflows_idle(timeout=2)

    workflow_duration_count = metrics.snapshot_histogram_count(
        MINION_WORKFLOW_DURATION_SECONDS,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_STATUS: "interrupted",
        },
    )
    step_duration_count = metrics.snapshot_histogram_count(
        MINION_WORKFLOW_STEP_DURATION_SECONDS,
        {
            LABEL_ORCHESTRATION_ID: m._mn_orchestration_id,
            LABEL_MINION: m._mn_minion_id,
            LABEL_MINION_WORKFLOW_STEP: "step_1",
            LABEL_STATUS: "interrupted",
        },
    )

    assert workflow_duration_count == 1
    assert step_duration_count == 1
    assert len(await state_store.get_all_contexts()) == 1


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
        minion_instance_id="iid",
        orchestration_id="ck",
        minion_module_path="tests.assets.continue_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.continue_persistence_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
        workflow_persistence_failure_policy="continue-on-failure",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    m._mn_started.set()
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
        minion_instance_id="iid",
        orchestration_id="ck",
        minion_module_path="tests.assets.idle_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.idle_persistence_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    m._mn_started.set()
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
        minion_instance_id="iid",
        orchestration_id="ck",
        minion_module_path="tests.assets.concurrent_idle_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.concurrent_idle_persistence_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_jitter_ratio=0.0,
    )

    m._mn_started.set()
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
        minion_instance_id="iid",
        orchestration_id="ck",
        minion_module_path="tests.assets.sustained_idle_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.sustained_idle_persistence_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_max_delay_seconds=0.04,
        workflow_persistence_retry_backoff_multiplier=2.0,
        workflow_persistence_retry_jitter_ratio=0.0,
        workflow_persistence_retry_warning_interval_seconds=0.02,
        workflow_persistence_retry_error_after_seconds=0.03,
    )

    m._mn_started.set()
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
        minion_instance_id="iid",
        orchestration_id="ck",
        minion_module_path="tests.assets.delete_blocking_success_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.delete_blocking_success_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
        workflow_persistence_failure_policy="continue-on-failure",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_max_delay_seconds=0.02,
        workflow_persistence_retry_backoff_multiplier=1.0,
        workflow_persistence_retry_jitter_ratio=0.0,
        workflow_persistence_retry_warning_interval_seconds=0.01,
        workflow_persistence_retry_error_after_seconds=None,
    )

    m._mn_started.set()
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


class SerializationFailureContext(msgspec.Struct):
    bad: int | None = None


@pytest.mark.asyncio
async def test_workflow_persistence_serialization_failure_is_non_retryable_and_preserves_prior_checkpoint(  # noqa: E501
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    step_calls: list[str] = []

    class UnserializableValue:
        pass

    class NonRetryablePersistenceMinion(Minion[EmptyEvent, SerializationFailureContext]):
        @minion_step
        async def step_1(self):
            step_calls.append("step_1")
            self.context.bad = UnserializableValue()  # pyright: ignore[reportAttributeAccessIssue]

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")

    m = NonRetryablePersistenceMinion(
        minion_instance_id="iid",
        orchestration_id="ck",
        minion_module_path="tests.assets.non_retryable_persistence_minion",
        config_path="cfg",
        state_store=state_store,
        metrics=metrics,
        logger=logger,
        minion_id="tests.assets.non_retryable_persistence_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    m._mn_started.set()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1"]
    persisted_contexts = await state_store.get_all_contexts()
    assert len(persisted_contexts) == 1
    decoded = deserialize_workflow_context_blob(persisted_contexts[0].context)
    assert decoded.next_step_index == 0
    assert decoded.context == SerializationFailureContext()
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


@pytest.mark.asyncio
async def test_minion_startup_replays_only_own_contexts(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class ReplayMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            return

    minion_module_path = "mock.module_path.minion_replay_shared"
    pipeline_id = "tests.assets.pipelines.shared"
    own_orchestration_id = f"{minion_module_path}|cfg-own|{pipeline_id}"
    other_orchestration_id = f"{minion_module_path}|cfg-other|{pipeline_id}"
    m = ReplayMinion(
        "iid",
        own_orchestration_id,
        minion_module_path,
        None,
        state_store,
        metrics,
        logger,
        minion_id=minion_module_path,
        minion_config_id="cfg-own",
        pipeline_id=pipeline_id,
    )

    await state_store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            orchestration_id=own_orchestration_id,
            workflow_id="wf-own",
            event=EmptyEvent(),
            context=EmptyContext(),
            context_cls=EmptyContext,
        )
    )
    await state_store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            orchestration_id=other_orchestration_id,
            workflow_id="wf-other",
            event=EmptyEvent(),
            context=EmptyContext(),
            context_cls=EmptyContext,
        )
    )

    replayed_ids: list[str] = []

    async def _capture(ctx: MinionWorkflowContext[EmptyEvent, EmptyContext]) -> None:
        replayed_ids.append(ctx.workflow_id)

    m._mn_run_workflow = _capture  # type: ignore[method-assign]

    await m._mn_startup()

    assert replayed_ids == ["wf-own"]


@pytest.mark.asyncio
async def test_minion_startup_replays_typed_msgspec_event_and_context(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    observed: list[tuple[type, type, int, int]] = []

    class ReplayEvent(msgspec.Struct):
        value: int

    class ReplayContext(msgspec.Struct):
        count: int = 0

    class ReplayMinion(Minion[ReplayEvent, ReplayContext]):
        @minion_step
        async def step_1(self):
            observed.append(
                (
                    type(self.event),
                    type(self.context),
                    self.event.value,
                    self.context.count,
                )
            )

    minion_module_path = "mock.module_path.minion_replay_typed"
    pipeline_id = "tests.assets.pipelines.shared"
    orchestration_id = f"{minion_module_path}|cfg|{pipeline_id}"
    m = ReplayMinion(
        "iid",
        orchestration_id,
        minion_module_path,
        None,
        state_store,
        metrics,
        logger,
        minion_id=minion_module_path,
        minion_config_id="cfg",
        pipeline_id=pipeline_id,
    )

    await state_store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            orchestration_id=orchestration_id,
            workflow_id="wf-typed",
            event=ReplayEvent(7),
            context=ReplayContext(11),
            context_cls=ReplayContext,
        )
    )

    await m._mn_startup()
    await m._mn_wait_until_workflows_idle(timeout=2)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [(ReplayEvent, ReplayContext, 7, 11)]


@pytest.mark.asyncio
async def test_runtime_guard_rejects_nested_step_invocation_via_indirect_call(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class NestedCallContext(msgspec.Struct):
        step2: str | None = None

    class NestedCallMinion(Minion[EmptyEvent, NestedCallContext]):
        @minion_step
        async def step_1(self):
            nested = getattr(self, "step_2")
            await nested()

        @minion_step
        async def step_2(self):
            self.context.step2 = "step2"

    m = NestedCallMinion(
        "iid",
        "ck",
        "tests.assets.nested_call_minion",
        None,
        state_store,
        metrics,
        logger,
        minion_id="tests.assets.nested_call_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
    )

    m._mn_started.set()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    failed_logs = [log for log in logger.logs if log.msg == "Workflow Step failed"]
    assert len(failed_logs) == 1
    assert failed_logs[0].kwargs["error_message"] == (
        "NestedCallMinion.step_2 cannot be called from within another @minion_step; "
        "workflow step sequencing is owned by the runtime workflow engine."
    )

    snap = metrics.snapshot()
    counters = snap.get("counter", {})
    step_failed_total = sum(
        s.get("value", 0) for s in counters.get(MINION_WORKFLOW_STEP_FAILED_TOTAL, [])
    )
    assert step_failed_total == 1


@pytest.mark.asyncio
async def test_minion_steps_can_access_event_and_context_across_workflow_steps(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    observed: list[tuple[str, int, object]] = []

    class AccessContext(msgspec.Struct):
        from_step_1: int | None = None

    class AccessMinion(Minion[IntValueEvent, AccessContext]):
        @minion_step
        async def step_1(self):
            event_value = self.event.value
            self.context.from_step_1 = event_value + 1
            observed.append(("step_1", event_value, self.context.from_step_1))

        @minion_step
        async def step_2(self):
            event_value = self.event.value
            observed.append(("step_2", event_value, self.context.from_step_1))

    m = AccessMinion(
        "iid",
        "ck",
        "tests.assets.access_minion",
        None,
        state_store,
        metrics,
        logger,
        minion_id="tests.assets.access_minion",
        minion_config_id="",
        pipeline_id="test-pipeline",
    )
    m._mn_started.set()
    await m._mn_handle_event(IntValueEvent(value=10))
    await m._mn_wait_until_workflows_idle(timeout=2)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [
        ("step_1", 10, 11),
        ("step_2", 10, 11),
    ]


@pytest.mark.asyncio
async def test_resumed_workflow_step_can_access_event_and_context_from_state_store(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    observed: list[tuple[str, int, object]] = []

    class ResumeAccessContext(msgspec.Struct):
        from_step_1: int | None = None

    class ResumeAccessMinion(Minion[IntValueEvent, ResumeAccessContext]):
        @minion_step
        async def step_1(self):
            pytest.fail("step_1 should not execute when replay resumes at next_step_index=1")

        @minion_step
        async def step_2(self):
            event_value = self.event.value
            observed.append(("step_2", event_value, self.context.from_step_1))

    minion_module_path = "tests.assets.resume_access_minion"
    pipeline_id = "tests.assets.pipelines.resume"
    orchestration_id = f"{minion_module_path}|cfg|{pipeline_id}"
    m = ResumeAccessMinion(
        "iid",
        orchestration_id,
        minion_module_path,
        None,
        state_store,
        metrics,
        logger,
        minion_id=minion_module_path,
        minion_config_id="cfg",
        pipeline_id=pipeline_id,
    )

    await state_store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            orchestration_id=orchestration_id,
            workflow_id="wf-resume",
            event=IntValueEvent(value=7),
            context=ResumeAccessContext(from_step_1=8),
            context_cls=ResumeAccessContext,
            next_step_index=1,
        )
    )

    await m._mn_startup()
    await m._mn_wait_until_workflows_idle(timeout=2)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [("step_2", 7, 8)]


@pytest.mark.asyncio
async def test_minion_startup_replay_skips_irrecoverable_context_and_replays_valid_context(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    observed: list[int] = []

    class ReplayWithInvalidContextMinion(Minion[IntValueEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            observed.append(self.event.value)

    minion_module_path = "tests.assets.replay_with_invalid_context_minion"
    pipeline_id = "tests.assets.pipelines.invalid"
    orchestration_id = f"{minion_module_path}|cfg|{pipeline_id}"
    m = ReplayWithInvalidContextMinion(
        "iid",
        orchestration_id,
        minion_module_path,
        None,
        state_store,
        metrics,
        logger,
        minion_id=minion_module_path,
        minion_config_id="cfg",
        pipeline_id=pipeline_id,
    )

    valid_context: MinionWorkflowContext[IntValueEvent, EmptyContext] = MinionWorkflowContext(
        orchestration_id=orchestration_id,
        workflow_id="wf-valid",
        event=IntValueEvent(value=123),
        context=EmptyContext(),
        context_cls=EmptyContext,
        next_step_index=0,
        started_at=None,
        error_msg=None,
    )
    invalid_context: MinionWorkflowContext[IntValueEvent, EmptyContext] = MinionWorkflowContext(
        orchestration_id=orchestration_id,
        workflow_id="wf-invalid",
        event=IntValueEvent(value=456),
        context=EmptyContext(),
        context_cls=EmptyContext,
        next_step_index=0,
        started_at=None,
        error_msg=None,
    )
    invalid_payload = PersistedMinionWorkflowContext(
        orchestration_id=invalid_context.orchestration_id,
        workflow_id=invalid_context.workflow_id,
        event=invalid_context.event,
        context=invalid_context.context,
        context_cls="builtins.dict",
        next_step_index=invalid_context.next_step_index,
        error_msg=invalid_context.error_msg,
        started_at=invalid_context.started_at,
        schema_version=999,
    )

    state_store._contexts["wf-valid"] = StoredWorkflowContext(
        workflow_id="wf-valid",
        orchestration_id=orchestration_id,
        context=serialize_persisted_workflow_context(valid_context),
    )
    state_store._contexts["wf-invalid"] = StoredWorkflowContext(
        workflow_id="wf-invalid",
        orchestration_id=orchestration_id,
        context=serialize(invalid_payload),
    )

    await m._mn_startup()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert observed == [123]
    assert logger.has_log(
        "StateStore failed to decode stored workflow context",
        log_kwargs={"error_type": "WorkflowContextSchemaError"},
    )


@pytest.mark.asyncio
async def test_minion_startup_replay_fails_closed_on_context_type_mismatch(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    observed: list[int] = []

    class StringValueEvent(msgspec.Struct):
        value: str

    class ReplayWithMismatchedContextMinion(Minion[IntValueEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            observed.append(self.event.value)

    minion_module_path = "tests.assets.replay_with_mismatched_context_minion"
    pipeline_id = "tests.assets.pipelines.invalid"
    orchestration_id = f"{minion_module_path}|cfg|{pipeline_id}"
    m = ReplayWithMismatchedContextMinion(
        "iid",
        orchestration_id,
        minion_module_path,
        None,
        state_store,
        metrics,
        logger,
        minion_id=minion_module_path,
        minion_config_id="cfg",
        pipeline_id=pipeline_id,
    )

    valid_context: MinionWorkflowContext[IntValueEvent, EmptyContext] = MinionWorkflowContext(
        orchestration_id=orchestration_id,
        workflow_id="wf-valid",
        event=IntValueEvent(value=123),
        context=EmptyContext(),
        context_cls=EmptyContext,
        next_step_index=0,
    )
    mismatched_context: MinionWorkflowContext[StringValueEvent, EmptyContext] = (
        MinionWorkflowContext(
            orchestration_id=orchestration_id,
            workflow_id="wf-mismatch",
            event=StringValueEvent(value="not-an-int"),
            context=EmptyContext(),
            context_cls=EmptyContext,
            next_step_index=0,
        )
    )

    state_store._contexts["wf-valid"] = StoredWorkflowContext(
        workflow_id="wf-valid",
        orchestration_id=orchestration_id,
        context=serialize_persisted_workflow_context(valid_context),
    )
    state_store._contexts["wf-mismatch"] = StoredWorkflowContext(
        workflow_id="wf-mismatch",
        orchestration_id=orchestration_id,
        context=serialize_persisted_workflow_context(mismatched_context),
    )

    with pytest.raises(Exception) as exc_info:
        await m._mn_startup()

    assert isinstance(exc_info.value.__cause__, WorkflowContextTypeMismatchError)
    assert observed == []
    assert logger.has_log(
        "StateStore failed to decode stored workflow context",
        log_kwargs={
            "workflow_id": "wf-mismatch",
            "error_type": "WorkflowContextTypeMismatchError",
        },
    )
