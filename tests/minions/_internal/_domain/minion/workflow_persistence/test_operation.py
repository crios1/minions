import asyncio
import random
from typing import Literal, NoReturn

import pytest

from minions import Minion
from minions._internal._domain.minion import WorkflowPersistencePoint
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_PERSISTENCE_ATTEMPTS_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_BLOCKED_GAUGE,
    MINION_WORKFLOW_PERSISTENCE_FAILURES_TOTAL,
    MINION_WORKFLOW_PERSISTENCE_SUCCEEDED_TOTAL,
)
from minions._internal._framework.state_store import StateStore
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_noop import NoOpMinion
from tests.assets.support.state_store_failable import FailableStateStore
from tests.assets.support.state_store_inmemory import InMemoryStateStore


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

    async def record_sleep(delay: float):
        sleep_delays.append(delay)
        if len(sleep_delays) == 4:
            store.save_failures.disable()

    def fail_if_jitter_is_applied(_lower: float, _upper: float) -> float:
        raise AssertionError("zero jitter must not sample randomness")

    monkeypatch.setattr(asyncio, "sleep", record_sleep)
    monkeypatch.setattr(random, "uniform", fail_if_jitter_is_applied)
    await m._mn_register_new_workflow_persistence_state(ctx)

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

    async def record_sleep(delay: float):
        sleep_delays.append(delay)
        if len(sleep_delays) == 2:
            store.save_failures.disable()

    def sample_each_bound(lower: float, upper: float) -> float:
        sampled_bounds.append((lower, upper))
        return lower if len(sampled_bounds) == 1 else upper

    monkeypatch.setattr(asyncio, "sleep", record_sleep)
    monkeypatch.setattr(random, "uniform", sample_each_bound)
    await m._mn_register_new_workflow_persistence_state(ctx)

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
    if operation == "save":
        await m._mn_register_new_workflow_persistence_state(ctx)
    else:
        await m._mn_register_resumed_workflow_persistence_state_if_absent(ctx)
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

    states = await m._mn_workflow_persistence_state_snapshot()
    if operation == "delete" and not fail:
        assert ctx.workflow_id not in states
    elif operation == "delete":
        assert states[ctx.workflow_id].risk_kind == "unresolved_delete"
    elif fail:
        assert states[ctx.workflow_id].risk_kind == "missing_checkpoint"
    else:
        assert states[ctx.workflow_id].risk_kind is None


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
    await m._mn_register_new_workflow_persistence_state(ctx)
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
