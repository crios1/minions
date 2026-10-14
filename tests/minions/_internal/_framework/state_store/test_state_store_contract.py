import asyncio
import time
from collections.abc import AsyncGenerator, Coroutine
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

import msgspec
import pytest
import pytest_asyncio

import minions._internal._framework.state_store as state_store_module
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.metrics_constants import (
    STATE_STORE_OPERATION_DURATION_SECONDS,
    STATE_STORE_OPERATION_FAILURES_TOTAL,
    STATE_STORE_OPERATIONS_TOTAL,
    STATE_STORE_PAYLOAD_SIZE_BYTES,
)
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.minion_workflow_context_codec import (
    CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION,
    PersistedMinionWorkflowContext,
    deserialize_workflow_context_blob,
    serialize_workflow_context,
)
from minions._internal._framework.state_store import (
    PersistenceOperationResult,
    StateStore,
    StoredWorkflowContext,
)
from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from minions._internal._utils.serialization import serialize
from tests.assets.crash.boom import BOOM_MESSAGE, BoomError
from tests.assets.crash.support.metrics.boom_metrics import (
    AssetMetrics as BoomMetrics,
)
from tests.assets.crash.support.state_store.boom_delete_context import (
    AssetStateStore as BoomDeleteContextStateStore,
)
from tests.assets.crash.support.state_store.boom_get_all_contexts import (
    AssetStateStore as BoomGetAllContextsStateStore,
)
from tests.assets.crash.support.state_store.boom_get_contexts_for_orchestration import (
    AssetStateStore as BoomGetContextsForOrchestrationStateStore,
)
from tests.assets.crash.support.state_store.boom_save_context import (
    AssetStateStore as BoomSaveContextStateStore,
)
from tests.assets.events.counter import CounterEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_failable import FailableStateStore
from tests.assets.support.state_store_inmemory import InMemoryStateStore

pytestmark = pytest.mark.asyncio


@dataclass
class DataclassEvent:
    value: int


@dataclass
class DataclassContext:
    total: int = 0


class MsgspecStructEvent(msgspec.Struct):
    value: int


class MsgspecStructContext(msgspec.Struct):
    total: int = 0


def mk_ctx(
    workflow_id: str = "dummy-workflow-id",
    *,
    orchestration_id: str = "dummy-orchestration-id",
    event: Any | None = None,
    context: Any | None = None,
    next_step_index: int = 0,
    error_msg: str | None = None,
) -> MinionWorkflowContext[Any, Any]:
    return MinionWorkflowContext(
        orchestration_id=orchestration_id,
        workflow_id=workflow_id,
        event={} if event is None else event,
        context={} if context is None else context,
        next_step_index=next_step_index,
        started_at=time.time(),
        error_msg=error_msg,
    )


@pytest_asyncio.fixture(params=["inmemory", "sqlite"], ids=["inmemory", "sqlite"])
async def state_store_under_contract(
    request: pytest.FixtureRequest,
    tmp_path: Path,
    logger: InMemoryLogger,
) -> AsyncGenerator[tuple[StateStore, InMemoryLogger], None]:
    if request.param == "sqlite":
        db_path = str(tmp_path / "state.db")
        store = SQLiteStateStore(
            db_path=db_path,
            logger=logger,
            metrics=NoOpMetrics(),
        )
        await logger._mn_startup()
        await store._mn_startup()
        try:
            yield store, logger
        finally:
            await store._mn_shutdown()
            await logger._mn_shutdown()
        return

    store = InMemoryStateStore(logger=logger, metrics=NoOpMetrics())
    yield store, logger


# Basic Contract


async def test_starts_empty(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract

    ctxs = await store._mn_get_all_decoded_contexts()

    assert ctxs == []


async def test_roundtrips_runtime_context(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract
    expected = mk_ctx(
        workflow_id="wf-roundtrip",
        event={"i": 1},
        context={"k": "v"},
        next_step_index=3,
    )

    await store._mn_serialize_and_save_context(expected)
    ctxs = await store._mn_get_all_decoded_contexts()

    assert len(ctxs) == 1
    actual = ctxs[0]
    assert actual == expected


async def test_overwrites_existing_workflow_context(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract
    workflow_id = "wf-overwrite"
    original = mk_ctx(
        workflow_id=workflow_id,
        event={"i": 1},
        context={"k": "original"},
        next_step_index=1,
    )
    updated = mk_ctx(
        workflow_id=workflow_id,
        event={"i": 2},
        context={"k": "updated"},
        next_step_index=4,
        error_msg="updated-error",
    )

    await store._mn_serialize_and_save_context(original)
    await store._mn_serialize_and_save_context(updated)
    ctxs = await store._mn_get_all_decoded_contexts()

    assert len(ctxs) == 1
    actual = ctxs[0]
    assert actual.workflow_id == workflow_id
    assert actual.orchestration_id == updated.orchestration_id
    assert actual.event == updated.event
    assert actual.context == updated.context
    assert type(actual.context) is dict
    assert actual.next_step_index == updated.next_step_index
    assert actual.error_msg == updated.error_msg


async def test_deletes_existing_workflow_context(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract
    workflow_id = "wf-delete"
    context = mk_ctx(
        workflow_id=workflow_id,
        event={"i": 1},
        context={"k": "v"},
        next_step_index=2,
    )

    await store._mn_serialize_and_save_context(context)
    await store._mn_delete_context(workflow_id)
    ctxs = await store._mn_get_all_decoded_contexts()

    assert all(ctx.workflow_id != workflow_id for ctx in ctxs)


async def test_delete_missing_workflow_context_is_noop(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract

    await store._mn_delete_context("wf-missing")
    ctxs = await store._mn_get_all_decoded_contexts()

    assert ctxs == []


# Raw Blob Contract


async def test_get_all_contexts_returns_blob_records(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract
    expected = mk_ctx(
        workflow_id="wf-record",
        event={"i": 1},
        context={"nested": {"value": "original"}},
    )

    await store._mn_serialize_and_save_context(expected)
    ctxs = await store.get_all_contexts()

    assert len(ctxs) == 1
    ctx = ctxs[0]
    assert ctx.workflow_id == expected.workflow_id
    assert ctx.orchestration_id == expected.orchestration_id
    persisted = msgspec.msgpack.decode(
        ctx.context,
        type=PersistedMinionWorkflowContext,
    )
    assert persisted.workflow_id == expected.workflow_id
    assert not hasattr(persisted, "minion_module_path")
    assert not hasattr(persisted, "minion_id")
    assert persisted.context_type_path == "builtins.dict"
    assert persisted.schema_version == CURRENT_WORKFLOW_CONTEXT_SCHEMA_VERSION
    decoded = deserialize_workflow_context_blob(ctx.context)
    assert decoded == expected


# Orchestration Filtering


async def test_get_contexts_for_orchestration_filters_by_identity(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, _ = state_store_under_contract
    target_a = mk_ctx(
        orchestration_id="dummy-target-a-orchestration-id",
        workflow_id="wf-target-a",
        event={"i": 1},
        context={"k": "a"},
    )
    target_b = mk_ctx(
        orchestration_id="dummy-target-b-orchestration-id",
        workflow_id="wf-target-b",
        event={"i": 2},
        context={"k": "b"},
        next_step_index=1,
    )
    other = mk_ctx(
        orchestration_id="dummy-other-orchestration-id",
        workflow_id="wf-other",
        event={"i": 3},
        context={"k": "c"},
        next_step_index=2,
    )

    await store._mn_serialize_and_save_context(target_a)
    await store._mn_serialize_and_save_context(target_b)
    await store._mn_serialize_and_save_context(other)

    filtered = await store._mn_get_decoded_contexts_for_orchestration(target_a.orchestration_id)
    filtered_ids = {ctx.workflow_id for ctx in filtered}
    assert filtered_ids == {"wf-target-a"}
    assert all(ctx.orchestration_id == target_a.orchestration_id for ctx in filtered)

    missing = await store._mn_get_decoded_contexts_for_orchestration(
        "dummy-missing-orchestration-id"
    )
    assert missing == []


# Typed Decode Contract


@pytest.mark.parametrize(
    ("event", "context"),
    [
        (DataclassEvent(10), DataclassContext(20)),
        (MsgspecStructEvent(10), MsgspecStructContext(20)),
    ],
    ids=["dataclass", "msgspec-struct"],
)
async def test_get_contexts_for_orchestration_restores_typed_models(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
    event: Any,
    context: Any,
):
    store, _ = state_store_under_contract
    event_cls = cast(type[Any], type(event))
    context_cls = cast(type[Any], type(context))
    expected = mk_ctx(
        workflow_id="wf-typed",
        event=event,
        context=context,
        next_step_index=2,
    )

    await store._mn_serialize_and_save_context(expected)
    ctxs = await store._mn_get_decoded_contexts_for_orchestration(
        expected.orchestration_id,
        event_cls=event_cls,
        context_cls=context_cls,
    )

    assert len(ctxs) == 1
    actual = ctxs[0]
    assert isinstance(actual.event, event_cls)
    assert isinstance(actual.context, context_cls)
    assert actual == expected


# Decode Failure Handling


async def test_skips_legacy_unversioned_blob(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, logger = state_store_under_contract
    legacy_payload = serialize_workflow_context(
        mk_ctx(
            workflow_id="wf-legacy-v1",
            event={"i": 99},
            context={"p": "legacy"},
            next_step_index=1,
        )
    )
    legacy_payload["step_index"] = legacy_payload.pop("next_step_index")
    legacy_payload.pop("schema_version", None)

    await store.save_context(
        "wf-legacy-v1",
        "dummy-orchestration-id",
        serialize(dict(legacy_payload)),
    )
    ctxs = await store._mn_get_all_decoded_contexts()

    assert all(c.workflow_id != "wf-legacy-v1" for c in ctxs)
    assert logger.has_log("StateStore failed to decode stored workflow context")


async def test_skips_unknown_schema_blob(
    state_store_under_contract: tuple[StateStore, InMemoryLogger],
):
    store, logger = state_store_under_contract
    unknown_ctx = mk_ctx(
        workflow_id="wf-unknown",
        event={"i": 100},
        context={"p": "unknown"},
    )
    unknown_payload = PersistedMinionWorkflowContext(
        orchestration_id=unknown_ctx.orchestration_id,
        workflow_id=unknown_ctx.workflow_id,
        event=unknown_ctx.event,
        context=unknown_ctx.context,
        context_type_path="builtins.dict",
        next_step_index=unknown_ctx.next_step_index,
        error_msg=unknown_ctx.error_msg,
        started_at=unknown_ctx.started_at,
        schema_version=999,
    )

    await store.save_context(
        "wf-unknown",
        "dummy-orchestration-id",
        serialize(unknown_payload),
    )
    ctxs = await store._mn_get_all_decoded_contexts()

    assert all(c.workflow_id != "wf-unknown" for c in ctxs)
    assert logger.has_log("StateStore failed to decode stored workflow context")


def make_context() -> MinionWorkflowContext[CounterEvent, dict[str, Any]]:
    return MinionWorkflowContext(
        orchestration_id="dummy-orchestration-id",
        workflow_id="dummy-workflow-id",
        event=CounterEvent(seq=1),
        context={},
    )


class TestStateStoreFailureContract:
    @pytest.mark.asyncio
    async def test_save_failure_returns_structured_result_and_logs(
        self,
        logger: InMemoryLogger,
    ):
        store = BoomSaveContextStateStore(logger=logger, metrics=NoOpMetrics())

        result = await store._mn_serialize_and_save_context(make_context())

        assert result == PersistenceOperationResult(
            persisted=False,
            failure_stage="save",
            error=result.error,
            retryable=True,
        )
        assert type(result.error).__name__ == "BoomError"
        assert logger.has_log("AssetStateStore.save_context failed")

    @pytest.mark.asyncio
    async def test_delete_failure_returns_structured_result_and_logs(
        self,
        logger: InMemoryLogger,
    ):
        store = BoomDeleteContextStateStore(logger=logger, metrics=NoOpMetrics())

        result = await store._mn_delete_context("wf-boom")

        assert result == PersistenceOperationResult(
            persisted=False,
            failure_stage="delete",
            error=result.error,
            retryable=True,
        )
        assert type(result.error).__name__ == "BoomError"
        assert logger.has_log("AssetStateStore.delete_context failed")

    @pytest.mark.asyncio
    async def test_get_contexts_for_orchestration_failure_raises_and_logs(
        self,
        logger: InMemoryLogger,
    ):
        store = BoomGetContextsForOrchestrationStateStore(
            logger=logger,
            metrics=NoOpMetrics(),
        )

        with pytest.raises(BoomError, match=BOOM_MESSAGE):
            await store._mn_get_contexts_for_orchestration("orch")
        assert logger.has_log("AssetStateStore.get_contexts_for_orchestration failed")

    @pytest.mark.asyncio
    async def test_get_all_contexts_failure_raises_and_logs(
        self,
        logger: InMemoryLogger,
    ):
        store = BoomGetAllContextsStateStore(logger=logger, metrics=NoOpMetrics())

        with pytest.raises(BoomError, match=BOOM_MESSAGE):
            await store._mn_get_all_contexts()
        assert logger.has_log("AssetStateStore.get_all_contexts failed")


class TestStateStoreTelemetryContract:
    @pytest.mark.asyncio
    async def test_state_store_operation_telemetry_records_operations_durations_and_payloads(
        self,
        logger: InMemoryLogger,
    ):
        metrics = InMemoryMetrics(logger=logger)
        store = InMemoryStateStore(logger=logger, metrics=metrics)

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
        assert (
            metrics.snapshot_histogram_count(
                STATE_STORE_OPERATION_DURATION_SECONDS,
                save_labels,
            )
            == 2
        )
        assert metrics.snapshot_histogram_sum(STATE_STORE_PAYLOAD_SIZE_BYTES, save_labels) == 5

        load_orchestration_labels = {
            "state_store_type": "InMemoryStateStore",
            "operation": "load_contexts_for_orchestration",
        }
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                load_orchestration_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_histogram_count(
                STATE_STORE_OPERATION_DURATION_SECONDS,
                load_orchestration_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_histogram_sum(
                STATE_STORE_PAYLOAD_SIZE_BYTES,
                load_orchestration_labels,
            )
            == 5
        )

        load_all_labels = {
            "state_store_type": "InMemoryStateStore",
            "operation": "load_all_contexts",
        }
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                load_all_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_histogram_count(
                STATE_STORE_OPERATION_DURATION_SECONDS,
                load_all_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_histogram_sum(
                STATE_STORE_PAYLOAD_SIZE_BYTES,
                load_all_labels,
            )
            == 5
        )

        delete_labels = {
            "state_store_type": "InMemoryStateStore",
            "operation": "delete_context",
        }
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                delete_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_histogram_count(
                STATE_STORE_OPERATION_DURATION_SECONDS,
                delete_labels,
            )
            == 1
        )
        metrics.assert_metric_label_observations_match_contract()

    @pytest.mark.asyncio
    async def test_state_store_write_failure_metrics_include_operation_and_error_type(
        self,
        logger: InMemoryLogger,
    ):
        metrics = InMemoryMetrics(logger=logger)
        store = FailableStateStore(logger=logger, metrics=metrics)
        store.save_failures.enable()

        await store._mn_save_context("wf-1", "orch-1", b"abc")
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                {"state_store_type": "FailableStateStore", "operation": "save_context"},
            )
            == 1
        )
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATION_FAILURES_TOTAL,
                {
                    "state_store_type": "FailableStateStore",
                    "operation": "save_context",
                    "error_type": "RuntimeError",
                },
            )
            == 1
        )

        store.save_failures.disable()
        store.delete_failures.enable()

        await store._mn_delete_context("wf-1")
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                {"state_store_type": "FailableStateStore", "operation": "delete_context"},
            )
            == 1
        )
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATION_FAILURES_TOTAL,
                {
                    "state_store_type": "FailableStateStore",
                    "operation": "delete_context",
                    "error_type": "RuntimeError",
                },
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_state_store_read_failure_metrics_include_operation_and_error_type(
        self,
        logger: InMemoryLogger,
        monkeypatch: pytest.MonkeyPatch,
    ):
        metrics = InMemoryMetrics(logger=logger)
        store = InMemoryStateStore(logger=logger, metrics=metrics)
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
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                load_orchestration_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATION_FAILURES_TOTAL,
                {
                    **load_orchestration_labels,
                    "error_type": "RuntimeError",
                },
            )
            == 1
        )

        load_all_labels = {
            "state_store_type": "InMemoryStateStore",
            "operation": "load_all_contexts",
        }
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATIONS_TOTAL,
                load_all_labels,
            )
            == 1
        )
        assert (
            metrics.snapshot_counter_value(
                STATE_STORE_OPERATION_FAILURES_TOTAL,
                {
                    **load_all_labels,
                    "error_type": "RuntimeError",
                },
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_state_store_failure_duration_excludes_error_logging(
        self,
        logger: InMemoryLogger,
        monkeypatch: pytest.MonkeyPatch,
    ):
        metrics = InMemoryMetrics(logger=logger)
        store = InMemoryStateStore(logger=logger, metrics=metrics)
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
            assert (
                metrics.snapshot_histogram_count(
                    STATE_STORE_OPERATION_DURATION_SECONDS,
                    labels,
                )
                == 1
            )
            assert (
                metrics.snapshot_histogram_sum(
                    STATE_STORE_OPERATION_DURATION_SECONDS,
                    labels,
                )
                == 1.0
            )

    @pytest.mark.asyncio
    async def test_canceled_state_store_operations_are_not_backend_failures(
        self,
        logger: InMemoryLogger,
        monkeypatch: pytest.MonkeyPatch,
    ):
        metrics = InMemoryMetrics(logger=logger)
        store = InMemoryStateStore(logger=logger, metrics=metrics)
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
        self,
        logger: InMemoryLogger,
    ):
        store = InMemoryStateStore(
            logger=logger,
            metrics=BoomMetrics(logger=logger),
        )

        result = await store._mn_save_context("wf-1", "orch-1", b"abc")

        assert result.persisted is True
        assert await store.get_contexts_for_orchestration("orch-1")
