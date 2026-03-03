import asyncio
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

from pprint import pprint

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
    state_store = InMemoryStateStore(logger=logger)

    # start the minion by calling _mn_handle_event directly with a dummy event
    m = AbortMinion('iid', 'ck', 'tests.assets.abort_minion', 'cfg', state_store, metrics, logger)
    await m._mn_handle_event({})

    # wait until the state store has recorded deletion (workflow finished)
    await state_store.wait_for_call('delete_context', count=1, timeout=2)

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
    state_store = InMemoryStateStore(logger=logger)

    m = FailMinion('iid', 'ck', 'tests.assets.fail_minion', 'cfg', state_store, metrics, logger)
    await m._mn_handle_event({})

    await state_store.wait_for_call('delete_context', count=1, timeout=2)

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
    state_store = InMemoryStateStore(logger=logger)

    from minions._internal._domain.minion_workflow_context import MinionWorkflowContext

    state_store._contexts = {
        "wf-own": MinionWorkflowContext(
            minion_modpath="mock.modpath.minion_replay_own",
            workflow_id="wf-own",
            event={},
            context={},
            context_cls=dict,
        ),
        "wf-other": MinionWorkflowContext(
            minion_modpath="mock.modpath.minion_replay_other",
            workflow_id="wf-other",
            event={},
            context={},
            context_cls=dict,
        ),
    }

    m = ReplayMinion(
        "iid",
        "ck",
        "mock.modpath.minion_replay_own",
        None,
        state_store,
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
    state_store = InMemoryStateStore(logger=logger)

    m = NestedCallMinion(
        "iid",
        "ck",
        "tests.assets.nested_call_minion",
        None,
        state_store,
        metrics,
        logger,
    )

    await m._mn_handle_event({})
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

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
    state_store = InMemoryStateStore(logger=logger)

    m = AccessMinion(
        "iid",
        "ck",
        "tests.assets.access_minion",
        None,
        state_store,
        metrics,
        logger,
    )
    await m._mn_handle_event({"value": 10})
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

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
            pytest.fail("step_1 should not execute when replay resumes at step_index=1")

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
    state_store = InMemoryStateStore(logger=logger)

    state_store._contexts = {
        "wf-resume": MinionWorkflowContext(
            minion_modpath="tests.assets.resume_access_minion",
            workflow_id="wf-resume",
            event={"value": 7},
            context={"from_step_1": 8},
            context_cls=dict,
            step_index=1,
        )
    }

    m = ResumeAccessMinion(
        "iid",
        "ck",
        "tests.assets.resume_access_minion",
        None,
        state_store,
        metrics,
        logger,
    )

    await m._mn_startup()
    await state_store.wait_for_call("delete_context", count=1, timeout=2)

    assert observed == [("step_2", 7, 8)]
