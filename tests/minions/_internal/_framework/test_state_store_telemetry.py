import asyncio
from collections.abc import Coroutine
from typing import Any

import pytest

import minions._internal._framework.state_store as state_store_module
from minions._internal._framework.metrics_constants import (
    STATE_STORE_OPERATION_DURATION_SECONDS,
    STATE_STORE_OPERATION_FAILURES_TOTAL,
    STATE_STORE_OPERATIONS_TOTAL,
    STATE_STORE_PAYLOAD_SIZE_BYTES,
)
from minions._internal._framework.state_store import StoredWorkflowContext
from tests.assets.crash.support.metrics.boom_child_operations import (
    AssetMetrics as BrokenMetrics,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_failable import FailableStateStore
from tests.assets.support.state_store_inmemory import InMemoryStateStore


@pytest.mark.asyncio
async def test_mn_record_operation_does_not_evaluate_payload_size_when_metrics_unbound(
    logger: InMemoryLogger,
):
    store = InMemoryStateStore(logger=logger)
    evaluated = False

    def payload_size_bytes() -> int:
        nonlocal evaluated
        evaluated = True
        return 3

    await store._mn_record_operation(
        "save_context",
        started_at=0.0,
        payload_size_bytes=payload_size_bytes,
    )

    assert evaluated is False


@pytest.mark.asyncio
async def test_state_store_operation_telemetry_records_operations_durations_and_payloads(
    logger: InMemoryLogger,
):
    metrics = InMemoryMetrics(logger=logger)
    store = InMemoryStateStore(logger=logger)
    store._mn_bind_metrics(metrics)

    await store._mn_save_context("wf-1", "orch-1", b"abc")
    await store._mn_save_context("wf-2", "orch-1", b"de")
    await store._mn_get_contexts_for_orchestration("orch-1")
    await store._mn_get_all_contexts()
    await store._mn_delete_context("wf-1")

    save_labels = {
        "state_store_type": "InMemoryStateStore",
        "operation": "save_context",
    }
    assert metrics.snapshot_counter_value(STATE_STORE_OPERATIONS_TOTAL, save_labels) == 2
    assert metrics.snapshot_histogram_count(
        STATE_STORE_OPERATION_DURATION_SECONDS,
        save_labels,
    ) == 2
    assert metrics.snapshot_histogram_sum(STATE_STORE_PAYLOAD_SIZE_BYTES, save_labels) == 5

    load_orchestration_labels = {
        "state_store_type": "InMemoryStateStore",
        "operation": "load_contexts_for_orchestration",
    }
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        load_orchestration_labels,
    ) == 1
    assert metrics.snapshot_histogram_count(
        STATE_STORE_OPERATION_DURATION_SECONDS,
        load_orchestration_labels,
    ) == 1
    assert metrics.snapshot_histogram_sum(
        STATE_STORE_PAYLOAD_SIZE_BYTES,
        load_orchestration_labels,
    ) == 5

    load_all_labels = {
        "state_store_type": "InMemoryStateStore",
        "operation": "load_all_contexts",
    }
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        load_all_labels,
    ) == 1
    assert metrics.snapshot_histogram_count(
        STATE_STORE_OPERATION_DURATION_SECONDS,
        load_all_labels,
    ) == 1
    assert metrics.snapshot_histogram_sum(
        STATE_STORE_PAYLOAD_SIZE_BYTES,
        load_all_labels,
    ) == 5

    delete_labels = {
        "state_store_type": "InMemoryStateStore",
        "operation": "delete_context",
    }
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        delete_labels,
    ) == 1
    assert metrics.snapshot_histogram_count(
        STATE_STORE_OPERATION_DURATION_SECONDS,
        delete_labels,
    ) == 1
    metrics.assert_metric_label_observations_match_contract()


@pytest.mark.asyncio
async def test_state_store_write_failure_metrics_include_operation_and_error_type(
    logger: InMemoryLogger,
):
    metrics = InMemoryMetrics(logger=logger)
    store = FailableStateStore(logger=logger)
    store._mn_bind_metrics(metrics)
    store.save_failures.enable()

    await store._mn_save_context("wf-1", "orch-1", b"abc")
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        {"state_store_type": "FailableStateStore", "operation": "save_context"},
    ) == 1
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATION_FAILURES_TOTAL,
        {
            "state_store_type": "FailableStateStore",
            "operation": "save_context",
            "error_type": "RuntimeError",
        },
    ) == 1

    store.save_failures.disable()
    store.delete_failures.enable()

    await store._mn_delete_context("wf-1")
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        {"state_store_type": "FailableStateStore", "operation": "delete_context"},
    ) == 1
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATION_FAILURES_TOTAL,
        {
            "state_store_type": "FailableStateStore",
            "operation": "delete_context",
            "error_type": "RuntimeError",
        },
    ) == 1


@pytest.mark.asyncio
async def test_state_store_read_failure_metrics_include_operation_and_error_type(
    logger: InMemoryLogger,
    monkeypatch: pytest.MonkeyPatch,
):
    metrics = InMemoryMetrics(logger=logger)
    store = InMemoryStateStore(logger=logger)
    store._mn_bind_metrics(metrics)
    load_error = RuntimeError("controlled load failure")
    load_all_error = RuntimeError("controlled all-contexts load failure")

    async def fail_load(self: InMemoryStateStore, orchestration_id: str):
        raise load_error

    async def fail_load_all(self: InMemoryStateStore):
        raise load_all_error

    monkeypatch.setattr(InMemoryStateStore, "get_contexts_for_orchestration", fail_load)
    monkeypatch.setattr(InMemoryStateStore, "get_all_contexts", fail_load_all)

    with pytest.raises(RuntimeError, match="controlled load failure"):
        await store._mn_get_contexts_for_orchestration("orch-1")
    with pytest.raises(RuntimeError, match="controlled all-contexts load failure"):
        await store._mn_get_all_contexts()

    load_orchestration_labels = {
        "state_store_type": "InMemoryStateStore",
        "operation": "load_contexts_for_orchestration",
    }
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        load_orchestration_labels,
    ) == 1
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATION_FAILURES_TOTAL,
        {
            **load_orchestration_labels,
            "error_type": "RuntimeError",
        },
    ) == 1

    load_all_labels = {
        "state_store_type": "InMemoryStateStore",
        "operation": "load_all_contexts",
    }
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATIONS_TOTAL,
        load_all_labels,
    ) == 1
    assert metrics.snapshot_counter_value(
        STATE_STORE_OPERATION_FAILURES_TOTAL,
        {
            **load_all_labels,
            "error_type": "RuntimeError",
        },
    ) == 1


@pytest.mark.asyncio
async def test_state_store_failure_duration_excludes_error_logging(
    logger: InMemoryLogger,
    monkeypatch: pytest.MonkeyPatch,
):
    metrics = InMemoryMetrics(logger=logger)
    store = InMemoryStateStore(logger=logger)
    store._mn_bind_metrics(metrics)
    clock = 0.0

    def perf_counter() -> float:
        return clock

    monkeypatch.setattr(state_store_module.time, "perf_counter", perf_counter)

    original_log_exception = logger._mn_log_exception

    async def log_exception(
        level: int,
        msg: str,
        exc: BaseException,
        **kwargs: Any,
    ) -> None:
        nonlocal clock
        clock += 100.0
        await original_log_exception(level, msg, exc, **kwargs)

    async def fail_save(
        self: InMemoryStateStore,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        nonlocal clock
        clock += 1.0
        raise RuntimeError("controlled save failure")

    async def fail_delete(self: InMemoryStateStore, workflow_id: str) -> None:
        nonlocal clock
        clock += 1.0
        raise RuntimeError("controlled delete failure")

    async def fail_load_for_orchestration(
        self: InMemoryStateStore,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        nonlocal clock
        clock += 1.0
        raise RuntimeError("controlled orchestration load failure")

    async def fail_load_all(
        self: InMemoryStateStore,
    ) -> list[StoredWorkflowContext]:
        nonlocal clock
        clock += 1.0
        raise RuntimeError("controlled all-contexts load failure")

    monkeypatch.setattr(logger, "_mn_log_exception", log_exception)
    monkeypatch.setattr(InMemoryStateStore, "save_context", fail_save)
    monkeypatch.setattr(InMemoryStateStore, "delete_context", fail_delete)
    monkeypatch.setattr(
        InMemoryStateStore,
        "get_contexts_for_orchestration",
        fail_load_for_orchestration,
    )
    monkeypatch.setattr(InMemoryStateStore, "get_all_contexts", fail_load_all)

    await store._mn_save_context("wf-1", "orch-1", b"abc")
    await store._mn_delete_context("wf-1")
    with pytest.raises(RuntimeError, match="orchestration load failure"):
        await store._mn_get_contexts_for_orchestration("orch-1")
    with pytest.raises(RuntimeError, match="all-contexts load failure"):
        await store._mn_get_all_contexts()

    for operation in (
        "save_context",
        "delete_context",
        "load_contexts_for_orchestration",
        "load_all_contexts",
    ):
        labels = {
            "state_store_type": "InMemoryStateStore",
            "operation": operation,
        }
        assert metrics.snapshot_histogram_count(
            STATE_STORE_OPERATION_DURATION_SECONDS,
            labels,
        ) == 1
        assert metrics.snapshot_histogram_sum(
            STATE_STORE_OPERATION_DURATION_SECONDS,
            labels,
        ) == 1.0


@pytest.mark.asyncio
async def test_canceled_state_store_operations_are_not_backend_failures(
    logger: InMemoryLogger,
    monkeypatch: pytest.MonkeyPatch,
):
    metrics = InMemoryMetrics(logger=logger)
    store = InMemoryStateStore(logger=logger)
    store._mn_bind_metrics(metrics)
    operation_started = asyncio.Event()
    release_operation = asyncio.Event()

    async def block_operation(*_args: object) -> None:
        operation_started.set()
        await release_operation.wait()

    monkeypatch.setattr(InMemoryStateStore, "save_context", block_operation)
    monkeypatch.setattr(InMemoryStateStore, "delete_context", block_operation)
    monkeypatch.setattr(
        InMemoryStateStore,
        "get_contexts_for_orchestration",
        block_operation,
    )
    monkeypatch.setattr(InMemoryStateStore, "get_all_contexts", block_operation)

    async def cancel_operation(operation: Coroutine[Any, Any, Any]) -> None:
        operation_started.clear()
        operation_task = asyncio.create_task(operation)

        await asyncio.wait_for(operation_started.wait(), timeout=1.0)
        operation_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await operation_task

    await cancel_operation(store._mn_save_context("wf-1", "orch-1", b"abc"))
    await cancel_operation(store._mn_delete_context("wf-1"))
    await cancel_operation(store._mn_get_contexts_for_orchestration("orch-1"))
    await cancel_operation(store._mn_get_all_contexts())

    assert metrics.snapshot_counter_value_total(STATE_STORE_OPERATIONS_TOTAL) == 0
    assert metrics.snapshot_counter_value_total(STATE_STORE_OPERATION_FAILURES_TOTAL) == 0
    assert metrics.snapshot_histogram_count_total(STATE_STORE_OPERATION_DURATION_SECONDS) == 0
    assert metrics.snapshot_histogram_count_total(STATE_STORE_PAYLOAD_SIZE_BYTES) == 0


@pytest.mark.asyncio
async def test_state_store_metric_failures_do_not_change_persistence(
    logger: InMemoryLogger,
):
    store = InMemoryStateStore(logger=logger)
    store._mn_bind_metrics(BrokenMetrics(logger=logger))

    result = await store._mn_save_context("wf-1", "orch-1", b"abc")

    assert result.persisted is True
    assert await store.get_contexts_for_orchestration("orch-1")
