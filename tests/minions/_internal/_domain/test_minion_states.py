import asyncio

import msgspec
import pytest
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    LABEL_MINION_COMPOSITE_KEY,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE,
    LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION,
    LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY,
    LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE,
    LABEL_STATUS,
    LABEL_STATE_STORE,
)
from minions._internal._framework.logger import ERROR

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from minions import minion_step, Minion
from minions._internal._domain.exceptions import AbortWorkflow
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
    serialize_workflow_context,
)
from minions._internal._framework.state_store import StoredWorkflowContext
from minions._internal._utils.serialization import serialize

from pprint import pprint


class ReplayEvent(msgspec.Struct):
    value: int


class ReplayContext(msgspec.Struct):
    count: int = 0


class IdleWaitMinion(Minion[dict, dict]):
    name = "idle-wait-minion"

    @minion_step
    async def step_1(self):
        return


async def _tracked_wait_task(
    m: Minion[dict, dict],
    event: asyncio.Event,
    *,
    aux: bool,
) -> asyncio.Task:
    task = asyncio.create_task(event.wait())
    async with m._mn_tasks_gate:
        if aux:
            m._mn_service_tasks.add(task)
        else:
            m._mn_workflow_tasks.add(task)
    task.add_done_callback(
        lambda t: (
            m._mn_service_tasks.discard(t)
            if aux else
            m._mn_workflow_tasks.discard(t)
        )
    )
    return task


async def _mark_minion_started_for_event_test(m: Minion[dict, dict]) -> None:
    m._mn_started.set()


async def _wait_until(condition, *, timeout: float = 1.0, poll_interval: float = 0.005) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        if condition():
            return
        await asyncio.sleep(poll_interval)
    raise TimeoutError("condition did not become true before timeout")


class FlakyPersistenceStateStore(InMemoryStateStore):
    def __init__(self, logger: InMemoryLogger, *, fail_attempts: set[int]):
        super().__init__(logger=logger)
        self.fail_attempts = set(fail_attempts)
        self.save_attempt_count = 0
        self.failed_attempts: list[int] = []
        self.persisted_next_step_indexes: list[int] = []

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        self.save_attempt_count += 1
        attempt = self.save_attempt_count
        if attempt in self.fail_attempts:
            self.failed_attempts.append(attempt)
            raise RuntimeError(f"intentional save failure at attempt {attempt}")

        decoded = deserialize_workflow_context_blob(context)
        self.persisted_next_step_indexes.append(decoded.next_step_index)
        await super().save_context(workflow_id, orchestration_id, context)


class FlakyDeletePersistenceStateStore(InMemoryStateStore):
    def __init__(self, logger: InMemoryLogger, *, fail_attempts: set[int]):
        super().__init__(logger=logger)
        self.fail_attempts = set(fail_attempts)
        self.delete_attempt_count = 0
        self.failed_attempts: list[int] = []

    async def delete_context(self, workflow_id: str) -> None:
        self.delete_attempt_count += 1
        attempt = self.delete_attempt_count
        if attempt in self.fail_attempts:
            self.failed_attempts.append(attempt)
            raise RuntimeError(f"intentional delete failure at attempt {attempt}")
        await super().delete_context(workflow_id)


def _sum_counter(metrics: InMemoryMetrics, metric_name: str) -> float:
    return sum(sample["value"] for sample in metrics.snapshot_counters().get(metric_name, []))


def _histogram_count(metrics: InMemoryMetrics, metric_name: str) -> float:
    return sum(sample["count"] for sample in metrics.snapshot_histograms().get(metric_name, []))


def _persistence_blocked_sample(metrics: InMemoryMetrics, labels: dict[str, str]):
    return InMemoryMetrics.find_sample(
        metrics.snapshot_gauges()[MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE],
        labels,
    )


@pytest.mark.asyncio
async def test_wait_until_tasks_idle_ignores_aux_tasks_by_default():
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)
    m = IdleWaitMinion("iid", "ck", "tests.assets.idle_wait_minion", None, store, metrics, logger)

    event = asyncio.Event()
    task = await _tracked_wait_task(m, event, aux=True)

    try:
        await m._mn_wait_until_tasks_idle(
            timeout=0.01,
            timeout_msg="IdleWaitMinion tasks did not become idle within 0.01s",
        )
        assert not task.done()
    finally:
        event.set()
        await task


@pytest.mark.asyncio
async def test_wait_until_tasks_idle_can_include_aux_tasks():
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)
    m = IdleWaitMinion("iid", "ck", "tests.assets.idle_wait_minion", None, store, metrics, logger)

    event = asyncio.Event()
    task = await _tracked_wait_task(m, event, aux=True)

    try:
        with pytest.raises(
            TimeoutError,
            match="IdleWaitMinion tasks did not become idle within 0.01s",
        ):
            await m._mn_wait_until_tasks_idle(
                timeout=0.01,
                include_aux_tasks=True,
                timeout_msg="IdleWaitMinion tasks did not become idle within 0.01s",
            )
    finally:
        event.set()
        await task


@pytest.mark.asyncio
async def test_workflow_aborted_increments_aborted_counter():
    # define a minion whose first step aborts
    class AbortMinion(Minion[dict, dict]):
        name = "abort-minion"

        @minion_step
        async def step_1(self):
            raise AbortWorkflow()

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)

    # start the minion by calling _mn_handle_event directly with a dummy event
    m = AbortMinion('iid', 'ck', 'tests.assets.abort_minion', 'cfg', store, metrics, logger)
    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await m._mn_wait_until_workflows_idle(timeout=2)

    # wait until the state store has recorded deletion (workflow finished)
    await store.wait_for_call('delete_context', count=1, timeout=2)

    snap = metrics.snapshot()
    pprint(snap)
    counters = snap.get('counter', {})
    aborted_total = sum(s.get("value", 0) for s in counters.get(MINION_WORKFLOW_ABORTED_TOTAL, []))

    # workflow started should be incremented and aborted counter incremented
    assert counters.get(MINION_WORKFLOW_STARTED_TOTAL)
    assert aborted_total == 1

    # no Gru instance created here


@pytest.mark.asyncio
async def test_workflow_failed_increments_failed_counter():
    class FailMinion(Minion[dict, dict]):
        name = "fail-minion"

        @minion_step
        async def step_1(self):
            raise RuntimeError('boom')

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)

    m = FailMinion('iid', 'ck', 'tests.assets.fail_minion', 'cfg', store, metrics, logger)
    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await m._mn_wait_until_workflows_idle(timeout=2)

    await store.wait_for_call('delete_context', count=1, timeout=2)

    snap = metrics.snapshot()
    pprint(snap)
    counters = snap.get('counter', {})
    failed_total = sum(s.get("value", 0) for s in counters.get(MINION_WORKFLOW_FAILED_TOTAL, []))

    assert counters.get(MINION_WORKFLOW_STARTED_TOTAL)
    assert failed_total == 1
    workflow_duration_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_histograms()[MINION_WORKFLOW_DURATION_SECONDS],
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_STATUS: "failed",
        },
    )
    step_duration_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_histograms()[MINION_WORKFLOW_STEP_DURATION_SECONDS],
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_MINION_WORKFLOW_STEP: "step_1",
            LABEL_STATUS: "failed",
        },
    )
    assert workflow_duration_sample["count"] == 1
    assert step_duration_sample["count"] == 1

    # nothing to shutdown (no Gru instance)


@pytest.mark.asyncio
async def test_workflow_cancellation_records_interrupted_duration_status_and_keeps_context():
    step_started = asyncio.Event()
    step_can_finish = asyncio.Event()

    class InterruptedMinion(Minion[dict, dict]):
        name = "interrupted-minion"

        @minion_step
        async def step_1(self):
            step_started.set()
            await step_can_finish.wait()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)

    m = InterruptedMinion("iid", "ck", "tests.assets.interrupted_minion", "cfg", store, metrics, logger)
    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await asyncio.wait_for(step_started.wait(), timeout=1.0)
    async with m._mn_tasks_gate:
        workflow_tasks = list(m._mn_workflow_tasks)
    assert len(workflow_tasks) == 1
    workflow_tasks[0].cancel()
    await m._mn_wait_until_workflows_idle(timeout=2)

    workflow_duration_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_histograms()[MINION_WORKFLOW_DURATION_SECONDS],
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_STATUS: "interrupted",
        },
    )
    step_duration_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_histograms()[MINION_WORKFLOW_STEP_DURATION_SECONDS],
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_MINION_WORKFLOW_STEP: "step_1",
            LABEL_STATUS: "interrupted",
        },
    )

    assert workflow_duration_sample["count"] == 1
    assert step_duration_sample["count"] == 1
    assert len(await store.get_all_contexts()) == 1


@pytest.mark.asyncio
async def test_workflow_persistence_continue_on_failure_advances_and_retries_at_next_checkpoint():
    step_calls: list[str] = []

    class ContinueOnFailureMinion(Minion[dict, dict]):
        name = "continue-persistence-minion"

        @minion_step
        async def step_1(self):
            step_calls.append("step_1")

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")

        @minion_step
        async def step_3(self):
            step_calls.append("step_3")

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = FlakyPersistenceStateStore(logger=logger, fail_attempts={3})
    m = ContinueOnFailureMinion(
        minion_instance_id="iid",
        minion_composite_key="ck",
        minion_modpath="tests.assets.continue_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        workflow_persistence_failure_policy="continue-on-failure",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1", "step_2", "step_3"]
    assert store.failed_attempts == [3]
    assert 2 in store.persisted_next_step_indexes
    assert logger.has_log("Workflow continuing after persistence failure")
    failure_log = next(log for log in logger.logs if log.msg == "Workflow continuing after persistence failure")
    assert failure_log.kwargs["persistence_failure_stage"] == "save"
    assert failure_log.kwargs["persistence_retryable"] is True
    assert failure_log.kwargs["suggestion"] == "Ensure the configured StateStore is available and can persist workflow context blobs."
    assert failure_log.kwargs["error_type"] == "RuntimeError"
    assert failure_log.kwargs[LABEL_STATE_STORE] == "FlakyPersistenceStateStore"
    assert failure_log.kwargs["event_type"] == "dict"
    assert failure_log.kwargs["context_type"] == "dict"
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 5
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 4
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 1
    assert _histogram_count(metrics, MINION_WORKFLOW_PERSISTENCE_DURATION_SECONDS) == 5
    assert MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE not in metrics.snapshot_gauges()


@pytest.mark.asyncio
async def test_workflow_persistence_idle_until_persisted_blocks_next_step_until_retry_succeeds():
    step_calls: list[str] = []
    step_1_done = asyncio.Event()
    step_2_started = asyncio.Event()

    class IdleUntilPersistedMinion(Minion[dict, dict]):
        name = "idle-persistence-minion"

        @minion_step
        async def step_1(self):
            step_calls.append("step_1")
            step_1_done.set()

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")
            step_2_started.set()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = FlakyPersistenceStateStore(logger=logger, fail_attempts={3, 4})
    m = IdleUntilPersistedMinion(
        minion_instance_id="iid",
        minion_composite_key="ck",
        minion_modpath="tests.assets.idle_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await asyncio.wait_for(step_1_done.wait(), timeout=1.0)
    await _wait_until(lambda: store.save_attempt_count >= 4, timeout=1.0)

    assert step_calls == ["step_1"]
    assert not step_2_started.is_set()

    await asyncio.wait_for(step_2_started.wait(), timeout=1.0)
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1", "step_2"]
    assert store.failed_attempts == [3, 4]
    assert logger.has_log("Workflow idled waiting for persistence")
    assert logger.has_log("Workflow persistence resumed")
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 6
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 4
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 2
    blocked_sample = _persistence_blocked_sample(
        metrics,
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "FlakyPersistenceStateStore",
        },
    )
    assert blocked_sample["value"] == 0


@pytest.mark.asyncio
async def test_workflow_persistence_blocked_gauge_counts_concurrent_workflows_for_same_label_set():
    step_calls = 0

    class ConcurrentIdleUntilPersistedMinion(Minion[dict, dict]):
        name = "concurrent-idle-persistence-minion"

        @minion_step
        async def step_1(self):
            nonlocal step_calls
            step_calls += 1

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = FlakyPersistenceStateStore(logger=logger, fail_attempts=set(range(3, 100)))
    m = ConcurrentIdleUntilPersistedMinion(
        minion_instance_id="iid",
        minion_composite_key="ck",
        minion_modpath="tests.assets.concurrent_idle_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_jitter_ratio=0.0,
    )

    await _mark_minion_started_for_event_test(m)
    await asyncio.gather(m._mn_handle_event({}), m._mn_handle_event({}))
    await _wait_until(lambda: store.save_attempt_count >= 4, timeout=1.0)

    labels = {
        LABEL_MINION_COMPOSITE_KEY: "ck",
        LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "before_step",
        LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "save",
        LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
        LABEL_STATE_STORE: "FlakyPersistenceStateStore",
    }
    assert _persistence_blocked_sample(metrics, labels)["value"] == 2
    assert step_calls == 0

    store.fail_attempts.clear()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == 2
    assert _persistence_blocked_sample(metrics, labels)["value"] == 0


@pytest.mark.asyncio
async def test_workflow_persistence_idle_until_persisted_relogs_and_escalates_sustained_failure():
    step_1_done = asyncio.Event()
    step_2_started = asyncio.Event()

    class SustainedIdleMinion(Minion[dict, dict]):
        name = "sustained-idle-persistence-minion"

        @minion_step
        async def step_1(self):
            step_1_done.set()

        @minion_step
        async def step_2(self):
            step_2_started.set()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = FlakyPersistenceStateStore(logger=logger, fail_attempts=set(range(3, 100)))
    m = SustainedIdleMinion(
        minion_instance_id="iid",
        minion_composite_key="ck",
        minion_modpath="tests.assets.sustained_idle_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_max_delay_seconds=0.04,
        workflow_persistence_retry_backoff_multiplier=2.0,
        workflow_persistence_retry_jitter_ratio=0.0,
        workflow_persistence_retry_warning_interval_seconds=0.02,
        workflow_persistence_retry_error_after_seconds=0.03,
    )

    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await asyncio.wait_for(step_1_done.wait(), timeout=1.0)
    assert not step_2_started.is_set()

    await _wait_until(
        lambda: len([log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"]) >= 3,
        timeout=1.0,
    )
    idle_logs = [log for log in logger.logs if log.msg == "Workflow idled waiting for persistence"]
    assert any(log.level == ERROR for log in idle_logs)
    assert idle_logs[-1].kwargs["persistence_retry_attempts"] > idle_logs[0].kwargs["persistence_retry_attempts"]
    assert {log.kwargs["persistence_retry_delay_seconds"] for log in idle_logs} <= {0.01, 0.02, 0.04}

    store.fail_attempts.clear()
    await asyncio.wait_for(step_2_started.wait(), timeout=1.0)
    await m._mn_wait_until_workflows_idle(timeout=2)


@pytest.mark.asyncio
async def test_workflow_success_is_delayed_until_checkpoint_delete_succeeds():
    step_1_done = asyncio.Event()

    class DeleteBlockingSuccessMinion(Minion[dict, dict]):
        name = "delete-blocking-success-minion"

        @minion_step
        async def step_1(self):
            step_1_done.set()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = FlakyDeletePersistenceStateStore(logger=logger, fail_attempts={1, 2})
    m = DeleteBlockingSuccessMinion(
        minion_instance_id="iid",
        minion_composite_key="ck",
        minion_modpath="tests.assets.delete_blocking_success_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        workflow_persistence_failure_policy="continue-on-failure",
        workflow_persistence_retry_delay_seconds=0.01,
        workflow_persistence_retry_max_delay_seconds=0.02,
        workflow_persistence_retry_backoff_multiplier=1.0,
        workflow_persistence_retry_jitter_ratio=0.0,
        workflow_persistence_retry_warning_interval_seconds=0.01,
        workflow_persistence_retry_error_after_seconds=None,
    )

    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await asyncio.wait_for(step_1_done.wait(), timeout=1.0)
    await _wait_until(lambda: store.delete_attempt_count >= 2, timeout=1.0)

    assert not logger.has_log("Workflow succeeded")
    assert _sum_counter(metrics, MINION_WORKFLOW_SUCCEEDED_TOTAL) == 0
    blocked_sample = _persistence_blocked_sample(
        metrics,
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "workflow_resolve",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "delete",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "delete",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "continue-on-failure",
            LABEL_STATE_STORE: "FlakyDeletePersistenceStateStore",
        },
    )
    assert blocked_sample["value"] == 1

    store.fail_attempts.clear()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert logger.has_log("Workflow idled waiting for checkpoint delete")
    assert logger.has_log("Workflow checkpoint delete resumed")
    assert logger.has_log("Workflow succeeded")
    assert _sum_counter(metrics, MINION_WORKFLOW_SUCCEEDED_TOTAL) == 1
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 5
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 3
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 2
    blocked_sample = _persistence_blocked_sample(
        metrics,
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "workflow_resolve",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "delete",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "delete",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "continue-on-failure",
            LABEL_STATE_STORE: "FlakyDeletePersistenceStateStore",
        },
    )
    assert blocked_sample["value"] == 0


@pytest.mark.asyncio
async def test_workflow_persistence_serialization_failure_is_non_retryable_and_preserves_prior_checkpoint():
    step_calls: list[str] = []

    class UnserializableValue:
        pass

    class NonRetryablePersistenceMinion(Minion[dict, dict]):
        name = "non-retryable-persistence-minion"

        @minion_step
        async def step_1(self):
            step_calls.append("step_1")
            self.context["bad"] = UnserializableValue()

        @minion_step
        async def step_2(self):
            step_calls.append("step_2")

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)
    m = NonRetryablePersistenceMinion(
        minion_instance_id="iid",
        minion_composite_key="ck",
        minion_modpath="tests.assets.non_retryable_persistence_minion",
        config_path="cfg",
        state_store=store,
        metrics=metrics,
        logger=logger,
        workflow_persistence_failure_policy="idle-until-persisted",
        workflow_persistence_retry_delay_seconds=0.01,
    )

    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert step_calls == ["step_1"]
    persisted_contexts = await store.get_all_contexts()
    assert len(persisted_contexts) == 1
    decoded = deserialize_workflow_context_blob(persisted_contexts[0].context)
    assert decoded.next_step_index == 0
    assert decoded.context == {}
    failure_log = next(log for log in logger.logs if log.msg == "Workflow persistence failed with non-retryable error")
    assert failure_log.kwargs["persistence_failure_stage"] == "serialize"
    assert failure_log.kwargs["persistence_retryable"] is False
    assert failure_log.kwargs["persistence_retry_delay_seconds"] is None
    assert failure_log.kwargs["suggestion"] == "Ensure workflow event and context values are supported by the Minions persistence codec."
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL) == 3
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL) == 2
    assert _sum_counter(metrics, MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL) == 1
    failure_samples = metrics.snapshot_counters()[MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL]
    failure_sample = InMemoryMetrics.find_sample(
        failure_samples,
        {
            LABEL_MINION_COMPOSITE_KEY: "ck",
            LABEL_MINION_WORKFLOW_PERSISTENCE_CHECKPOINT_TYPE: "before_step",
            LABEL_MINION_WORKFLOW_PERSISTENCE_OPERATION: "save",
            LABEL_MINION_WORKFLOW_PERSISTENCE_FAILURE_STAGE: "serialize",
            LABEL_MINION_WORKFLOW_PERSISTENCE_RETRYABLE: "false",
            LABEL_MINION_WORKFLOW_PERSISTENCE_POLICY: "idle-until-persisted",
            LABEL_STATE_STORE: "InMemoryStateStore",
        },
    )
    assert failure_sample["value"] == 1


@pytest.mark.asyncio
async def test_minion_startup_replays_only_own_contexts():
    class ReplayMinion(Minion[dict, dict]):
        name = "replay-minion"

        @minion_step
        async def step_1(self):
            return

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)
    minion_modpath = "mock.modpath.minion_replay_shared"
    own_composite_key = f"{minion_modpath}|cfg-own|tests.assets.pipelines.shared"
    other_composite_key = f"{minion_modpath}|cfg-other|tests.assets.pipelines.shared"

    await store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            minion_composite_key=own_composite_key,
            minion_modpath=minion_modpath,
            workflow_id="wf-own",
            event={},
            context={},
            context_cls=dict,
        )
    )
    await store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            minion_composite_key=other_composite_key,
            minion_modpath=minion_modpath,
            workflow_id="wf-other",
            event={},
            context={},
            context_cls=dict,
        )
    )

    m = ReplayMinion(
        "iid",
        own_composite_key,
        minion_modpath,
        None,
        store,
        metrics,
        logger,
    )

    replayed_ids: list[str] = []

    async def _capture(ctx):
        replayed_ids.append(ctx.workflow_id)

    m._mn_run_workflow = _capture  # type: ignore[method-assign]

    await m._mn_startup()

    assert replayed_ids == ["wf-own"]


@pytest.mark.asyncio
async def test_minion_startup_replays_typed_msgspec_event_and_context():
    observed: list[tuple[type, type, int, int]] = []

    class ReplayMinion(Minion[ReplayEvent, ReplayContext]):
        name = "replay-minion-typed"

        @minion_step
        async def step_1(self):
            observed.append((
                type(self.event),
                type(self.context),
                self.event.value,
                self.context.count,
            ))

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()
    store = InMemoryStateStore(logger=logger)

    await store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            minion_composite_key="mock.modpath.minion_replay_typed|cfg|tests.assets.pipelines.shared",
            minion_modpath="mock.modpath.minion_replay_typed",
            workflow_id="wf-typed",
            event=ReplayEvent(7),
            context=ReplayContext(11),
            context_cls=ReplayContext,
        )
    )

    m = ReplayMinion(
        "iid",
        "mock.modpath.minion_replay_typed|cfg|tests.assets.pipelines.shared",
        "mock.modpath.minion_replay_typed",
        None,
        store,
        metrics,
        logger,
    )

    await m._mn_startup()
    await m._mn_wait_until_workflows_idle(timeout=2)
    await store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [(ReplayEvent, ReplayContext, 7, 11)]


@pytest.mark.asyncio
async def test_runtime_guard_rejects_nested_step_invocation_via_indirect_call():
    class NestedCallMinion(Minion[dict, dict]):
        name = "nested-call-minion"

        @minion_step
        async def step_1(self):
            nested = getattr(self, "step_2")
            await nested()

        @minion_step
        async def step_2(self):
            self.context["step2"] = "step2"

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)

    m = NestedCallMinion(
        "iid",
        "ck",
        "tests.assets.nested_call_minion",
        None,
        store,
        metrics,
        logger,
    )

    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({})
    await m._mn_wait_until_workflows_idle(timeout=2)
    await store.wait_for_call("delete_context", count=1, timeout=2)

    failed_logs = [log for log in logger.logs if log.msg == "Workflow Step failed"]
    assert len(failed_logs) == 1
    assert failed_logs[0].kwargs["error_message"] == (
        "NestedCallMinion.step_2 cannot be called from within another @minion_step; "
        "workflow step sequencing is owned by the runtime workflow engine."
    )

    snap = metrics.snapshot()
    counters = snap.get("counter", {})
    step_failed_total = sum(
        s.get("value", 0)
        for s in counters.get(MINION_WORKFLOW_STEP_FAILED_TOTAL, [])
    )
    assert step_failed_total == 1


@pytest.mark.asyncio
async def test_minion_steps_can_access_event_and_context_across_workflow_steps():
    observed: list[tuple[str, int, int | None]] = []

    class AccessMinion(Minion[dict, dict]):
        name = "access-minion"

        @minion_step
        async def step_1(self):
            event_value = self.event["value"]
            self.context["from_step_1"] = event_value + 1
            observed.append(("step_1", event_value, self.context.get("from_step_1")))

        @minion_step
        async def step_2(self):
            event_value = self.event["value"]
            observed.append(("step_2", event_value, self.context.get("from_step_1")))

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)

    m = AccessMinion(
        "iid",
        "ck",
        "tests.assets.access_minion",
        None,
        store,
        metrics,
        logger,
    )
    await _mark_minion_started_for_event_test(m)
    await m._mn_handle_event({"value": 10})
    await m._mn_wait_until_workflows_idle(timeout=2)
    await store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [
        ("step_1", 10, 11),
        ("step_2", 10, 11),
    ]


@pytest.mark.asyncio
async def test_resumed_workflow_step_can_access_event_and_context_from_state_store():
    observed: list[tuple[str, int, int | None]] = []

    class ResumeAccessMinion(Minion[dict, dict]):
        name = "resume-access-minion"

        @minion_step
        async def step_1(self):
            pytest.fail("step_1 should not execute when replay resumes at next_step_index=1")

        @minion_step
        async def step_2(self):
            event_value = self.event["value"]
            observed.append(("step_2", event_value, self.context.get("from_step_1")))

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset_spy()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset_spy()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)

    await store._mn_serialize_and_save_context(
        MinionWorkflowContext(
            minion_composite_key="tests.assets.resume_access_minion|cfg|tests.assets.pipelines.resume",
            minion_modpath="tests.assets.resume_access_minion",
            workflow_id="wf-resume",
            event={"value": 7},
            context={"from_step_1": 8},
            context_cls=dict,
            next_step_index=1,
        )
    )

    m = ResumeAccessMinion(
        "iid",
        "tests.assets.resume_access_minion|cfg|tests.assets.pipelines.resume",
        "tests.assets.resume_access_minion",
        None,
        store,
        metrics,
        logger,
    )

    await m._mn_startup()
    await m._mn_wait_until_workflows_idle(timeout=2)
    await store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [("step_2", 7, 8)]

@pytest.mark.asyncio
async def test_minion_startup_replay_skips_irrecoverable_context_and_replays_valid_context():
    observed: list[int] = []

    class ReplayWithInvalidContextMinion(Minion[dict, dict]):
        name = "replay-with-invalid-context-minion"

        @minion_step
        async def step_1(self):
            observed.append(self.event["value"])

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    store = InMemoryStateStore(logger=logger)
    minion_modpath = "tests.assets.replay_with_invalid_context_minion"

    valid_payload = serialize_workflow_context(
        MinionWorkflowContext(
            minion_composite_key=f"{minion_modpath}|cfg|tests.assets.pipelines.invalid",
            minion_modpath=minion_modpath,
            workflow_id="wf-valid",
            event={"value": 123},
            context={},
            context_cls=dict,
            next_step_index=0,
            started_at=None,
            error_msg=None,
        )
    )
    invalid_payload = serialize_workflow_context(
        MinionWorkflowContext(
            minion_composite_key=f"{minion_modpath}|cfg|tests.assets.pipelines.invalid",
            minion_modpath=minion_modpath,
            workflow_id="wf-invalid",
            event={"value": 456},
            context={},
            context_cls=dict,
            next_step_index=0,
            started_at=None,
            error_msg=None,
        )
    )
    invalid_payload["schema_version"] = 999

    store._contexts["wf-valid"] = StoredWorkflowContext(
        workflow_id="wf-valid",
        orchestration_id=f"{minion_modpath}|cfg|tests.assets.pipelines.invalid",
        context=serialize(dict(valid_payload)),
    )
    store._contexts["wf-invalid"] = StoredWorkflowContext(
        workflow_id="wf-invalid",
        orchestration_id=f"{minion_modpath}|cfg|tests.assets.pipelines.invalid",
        context=serialize(dict(invalid_payload)),
    )

    m = ReplayWithInvalidContextMinion(
        "iid",
        f"{minion_modpath}|cfg|tests.assets.pipelines.invalid",
        minion_modpath,
        None,
        store,
        metrics,
        logger,
    )

    await m._mn_startup()
    await m._mn_wait_until_workflows_idle(timeout=2)

    assert observed == [123]
    assert logger.has_log("StateStore failed to decode stored workflow context")
