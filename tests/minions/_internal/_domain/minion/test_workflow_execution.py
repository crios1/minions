import asyncio
from dataclasses import FrozenInstanceError
from typing import Any

import pytest

from minions import Minion, minion_step
from minions._internal._domain.exceptions import AbortWorkflow
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_ORCHESTRATION_ID,
    LABEL_STATUS,
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
)
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from minions.types import MinionWorkflowHandle
from tests.assets.contexts.empty import EmptyContext
from tests.assets.contexts.int_value import IntValueContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.events.int_value import IntValueEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


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

    # Exercise workflow handling directly without startup replay or a service task.
    m = AbortMinion(
        "dummy-minion-instance-id",
        "dummy-orchestration-id",
        "dummy-minion-module-path",
        "dummy-config-path",
        state_store,
        metrics,
        logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )
    m._mn_mark_running()
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
async def test_workflow_failure_is_terminal_deletes_checkpoint_and_increments_failed_counter(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class FailMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            raise RuntimeError("boom")

    m = FailMinion(
        "dummy-minion-instance-id",
        "dummy-orchestration-id",
        "dummy-minion-module-path",
        "dummy-config-path",
        state_store,
        metrics,
        logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
        workflow_failure_policy="delete",
    )
    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)

    await state_store.wait_for_call("delete_context", count=1, timeout=2)
    assert await state_store.get_all_contexts() == []

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
        "dummy-minion-instance-id",
        "dummy-orchestration-id",
        "dummy-minion-module-path",
        "dummy-config-path",
        state_store,
        metrics,
        logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )
    m._mn_mark_running()
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
async def test_runtime_guard_rejects_nested_step_invocation_via_indirect_call(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class MyMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self):
            nested = getattr(self, "step_2")
            await nested()

        @minion_step
        async def step_2(self):
            pass

    m = MyMinion(
        "dummy-minion-instance-id",
        "dummy-orchestration-id",
        "dummy-minion-module-path",
        None,
        state_store,
        metrics,
        logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )

    m._mn_mark_running()
    await m._mn_handle_event(EmptyEvent())
    await m._mn_wait_until_workflows_idle(timeout=2)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    failed_logs = [log for log in logger.logs if log.msg == "Workflow Step failed"]
    assert len(failed_logs) == 1
    assert failed_logs[0].kwargs["error_message"] == (
        "MyMinion.step_2 cannot be called from within another @minion_step; "
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

    class MyMinion(Minion[IntValueEvent, IntValueContext]):
        @minion_step
        async def step_1(self):
            event_value = self.event.value
            self.context.value = event_value + 1
            observed.append(("step_1", event_value, self.context.value))

        @minion_step
        async def step_2(self):
            event_value = self.event.value
            observed.append(("step_2", event_value, self.context.value))

    m = MyMinion(
        "dummy-minion-instance-id",
        "dummy-orchestration-id",
        "dummy-minion-module-path",
        None,
        state_store,
        metrics,
        logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )
    m._mn_mark_running()
    await m._mn_handle_event(IntValueEvent(value=10))
    await m._mn_wait_until_workflows_idle(timeout=2)
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [
        ("step_1", 10, 11),
        ("step_2", 10, 11),
    ]


class TestMinionWorkflowHandle:
    @pytest.mark.asyncio
    async def test_workflow_handle_is_available_during_workflow_steps(self):
        captured_handles: list[MinionWorkflowHandle] = []

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                captured_handles.append(self.workflow_handle)

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )
        m._mn_mark_running()

        await m._mn_handle_event(EmptyEvent())
        await m._mn_wait_until_tasks_idle(timeout=1.0, timeout_msg="workflow did not finish")

        assert len(captured_handles) == 1
        assert captured_handles[0].orchestration_id == "dummy-orchestration-id"
        assert isinstance(captured_handles[0].workflow_id, str)
        assert captured_handles[0].workflow_id

    @pytest.mark.asyncio
    async def test_workflow_handle_values_match_framework_log_and_state_identity(
        self,
        logger: InMemoryLogger,
        state_store: InMemoryStateStore,
    ):
        captured_handles: list[MinionWorkflowHandle] = []
        captured_stored_contexts: list[Any] = []

        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self):
                captured_handles.append(self.workflow_handle)
                captured_stored_contexts.extend(await state_store.get_all_contexts())

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path=None,
            state_store=state_store,
            metrics=NoOpMetrics(),
            logger=logger,
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )
        m._mn_mark_running()

        await m._mn_handle_event(EmptyEvent())
        await m._mn_wait_until_tasks_idle(timeout=1.0, timeout_msg="workflow did not finish")

        assert len(captured_handles) == 1
        handle = captured_handles[0]
        assert captured_stored_contexts
        assert all(
            stored.orchestration_id == handle.orchestration_id
            for stored in captured_stored_contexts
        )
        assert all(stored.workflow_id == handle.workflow_id for stored in captured_stored_contexts)
        assert await state_store.get_all_contexts() == []
        workflow_started = next(log for log in logger.logs if log.msg == "Workflow started")
        assert workflow_started.kwargs["orchestration_id"] == handle.orchestration_id
        assert workflow_started.kwargs["workflow_id"] == handle.workflow_id

    def test_workflow_handle_is_read_only(self):
        handle = MinionWorkflowHandle(
            orchestration_id="dummy-orchestration-id",
            workflow_id="dummy-workflow-id",
        )

        with pytest.raises(FrozenInstanceError):
            handle.workflow_id = "workflow-2"  # pyright: ignore[reportAttributeAccessIssue]

    def test_workflow_handle_outside_active_workflow_raises_clear_error(self):
        class MyMinion(Minion[EmptyEvent, EmptyContext]):
            @minion_step
            async def step_1(self): ...

        m = MyMinion(
            minion_instance_id="dummy-minion-instance-id",
            orchestration_id="dummy-orchestration-id",
            minion_module_path="dummy-minion-module-path",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=NoOpLogger(),
            minion_id="dummy-minion-id",
            minion_config_id="",
            pipeline_id="dummy-pipeline-id",
        )

        with pytest.raises(
            RuntimeError,
            match="No workflow handle is currently bound to this workflow",
        ):
            m.workflow_handle
