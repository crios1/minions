import asyncio

import msgspec
import pytest
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_ABORTED_TOTAL,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
    LABEL_MINION
)

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from minions import minion_step, Minion
from minions._internal._domain.exceptions import AbortWorkflow
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
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

    # nothing to shutdown (no Gru instance)


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
