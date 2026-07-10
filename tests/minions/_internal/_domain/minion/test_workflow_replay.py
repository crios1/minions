import msgspec
import pytest

from minions import Minion, minion_step
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    PersistedMinionWorkflowContext,
    WorkflowContextTypeMismatchError,
    serialize_persisted_workflow_context,
)
from minions._internal._framework.state_store import StoredWorkflowContext
from minions._internal._utils.serialization import serialize
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.events.int_value import IntValueEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


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
