from dataclasses import dataclass
import os
import time
from collections.abc import AsyncGenerator

import msgspec
import pytest
import pytest_asyncio

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
    persist_workflow_context,
    serialize_workflow_context,
)
from minions._internal._framework.state_store import StateStore
from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from minions._internal._utils.serialization import serialize
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.state_store_inmemory import InMemoryStateStore


pytestmark = pytest.mark.asyncio


@dataclass
class EventDC:
    value: int


@dataclass
class ContextDC:
    total: int = 0


class EventStruct(msgspec.Struct):
    value: int


class ContextStruct(msgspec.Struct):
    total: int = 0


def _ck(minion_modpath: str, *, config: str = "cfg", pipeline: str = "app.pipeline") -> str:
    return f"{minion_modpath}|{config}|{pipeline}"


def _blob_for(ctx: MinionWorkflowContext) -> bytes:
    return serialize(persist_workflow_context(ctx))


def mk_ctx(
    workflow_id: str = "wf-0",
    *,
    minion_modpath: str = "app.minion",
    config: str = "cfg",
    pipeline: str = "app.pipeline",
    event: object | None = None,
    context: object | None = None,
    context_cls: type = dict,
    next_step_index: int = 0,
    error_msg: str | None = None,
) -> MinionWorkflowContext:
    return MinionWorkflowContext(
        minion_composite_key=_ck(minion_modpath, config=config, pipeline=pipeline),
        minion_modpath=minion_modpath,
        workflow_id=workflow_id,
        event={} if event is None else event,
        context={} if context is None else context,
        context_cls=context_cls,
        next_step_index=next_step_index,
        started_at=time.time(),
        error_msg=error_msg,
    )


@pytest_asyncio.fixture(params=["inmemory", "sqlite"], ids=["inmemory", "sqlite"])
async def store_and_logger(
    request: pytest.FixtureRequest, tmp_path: str
) -> AsyncGenerator[tuple[StateStore, InMemoryLogger], None]:
    logger = InMemoryLogger()

    if request.param == "sqlite":
        db_path = os.path.join(tmp_path, "state.db")
        store = SQLiteStateStore(db_path=db_path, logger=logger)
        await logger._mn_startup()
        await store._mn_startup()
        try:
            yield store, logger
        finally:
            await store._mn_shutdown()
            await logger._mn_shutdown()
        return

    store = InMemoryStateStore(logger=logger)
    yield store, logger


# Basic Contract


async def test_state_store_starts_empty(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger

    ctxs = await store._mn_get_all_decoded_contexts()

    assert ctxs == []


async def test_state_store_roundtrips_runtime_context(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
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
    assert actual.workflow_id == expected.workflow_id
    assert actual.minion_composite_key == expected.minion_composite_key
    assert actual.minion_modpath == expected.minion_modpath
    assert actual.event == expected.event
    assert actual.context == expected.context
    assert actual.context_cls is dict
    assert actual.next_step_index == expected.next_step_index
    assert actual.error_msg == expected.error_msg


async def test_state_store_overwrites_existing_workflow_context(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    workflow_id = "wf-overwrite"
    original = mk_ctx(
        workflow_id=workflow_id,
        event={"i": 1},
        context={"k": "original"},
        next_step_index=1,
    )
    updated = mk_ctx(
        minion_modpath="app.minion.updated",
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
    assert actual.minion_composite_key == updated.minion_composite_key
    assert actual.minion_modpath == updated.minion_modpath
    assert actual.event == updated.event
    assert actual.context == updated.context
    assert actual.context_cls is dict
    assert actual.next_step_index == updated.next_step_index
    assert actual.error_msg == updated.error_msg


async def test_state_store_deletes_existing_workflow_context(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
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


async def test_state_store_delete_missing_workflow_context_is_noop(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger

    await store._mn_delete_context("wf-missing")
    ctxs = await store._mn_get_all_decoded_contexts()

    assert ctxs == []


# Raw Blob Contract


async def test_state_store_get_all_contexts_returns_blob_records(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
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
    assert ctx.orchestration_id == expected.minion_composite_key
    assert deserialize_workflow_context_blob(ctx.context) == expected


# Orchestration Filtering


async def test_state_store_get_contexts_for_orchestration_filters_by_identity(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    target_modpath = "app.minion.shared"
    target_a = mk_ctx(
        minion_modpath=target_modpath,
        config="cfg-a",
        workflow_id="wf-target-a",
        event={"i": 1},
        context={"k": "a"},
    )
    target_b = mk_ctx(
        minion_modpath=target_modpath,
        config="cfg-b",
        workflow_id="wf-target-b",
        event={"i": 2},
        context={"k": "b"},
        next_step_index=1,
    )
    other = mk_ctx(
        minion_modpath=target_modpath,
        config="cfg-c",
        workflow_id="wf-other",
        event={"i": 3},
        context={"k": "c"},
        next_step_index=2,
    )

    await store._mn_serialize_and_save_context(target_a)
    await store._mn_serialize_and_save_context(target_b)
    await store._mn_serialize_and_save_context(other)

    filtered = await store._mn_get_decoded_contexts_for_orchestration(target_a.minion_composite_key)
    filtered_ids = {ctx.workflow_id for ctx in filtered}
    assert filtered_ids == {"wf-target-a"}
    assert all(ctx.minion_composite_key == target_a.minion_composite_key for ctx in filtered)
    assert all(ctx.minion_modpath == target_modpath for ctx in filtered)

    missing = await store._mn_get_decoded_contexts_for_orchestration(_ck(target_modpath, config="cfg-missing"))
    assert missing == []


# Typed Decode Contract


@pytest.mark.parametrize(
    ("event", "context", "event_cls", "context_cls"),
    [
        (EventDC(10), ContextDC(20), EventDC, ContextDC),
        (EventStruct(10), ContextStruct(20), EventStruct, ContextStruct),
    ],
)
async def test_state_store_get_contexts_for_orchestration_restores_typed_models(
    store_and_logger: tuple[StateStore, InMemoryLogger],
    event,
    context,
    event_cls,
    context_cls,
):
    store, _ = store_and_logger
    expected = mk_ctx(
        minion_modpath="app.minion.typed",
        workflow_id="wf-typed",
        event=event,
        context=context,
        context_cls=context_cls,
        next_step_index=2,
    )

    await store._mn_serialize_and_save_context(expected)
    ctxs = await store._mn_get_decoded_contexts_for_orchestration(
        expected.minion_composite_key,
        event_cls=event_cls,
        context_cls=context_cls,
    )

    assert len(ctxs) == 1
    actual = ctxs[0]
    assert isinstance(actual.event, event_cls)
    assert isinstance(actual.context, context_cls)
    assert actual == expected


# Decode Failure Handling


async def test_state_store_skips_legacy_unversioned_blob(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, logger = store_and_logger
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
        _ck("app.minion"),
        serialize(dict(legacy_payload)),
    )
    ctxs = await store._mn_get_all_decoded_contexts()

    assert all(c.workflow_id != "wf-legacy-v1" for c in ctxs)
    assert logger.has_log("StateStore failed to decode stored workflow context")


async def test_state_store_skips_unknown_schema_blob(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, logger = store_and_logger
    unknown_payload = serialize_workflow_context(
        mk_ctx(
            workflow_id="wf-unknown",
            event={"i": 100},
            context={"p": "unknown"},
        )
    )
    unknown_payload["schema_version"] = 999

    await store.save_context(
        "wf-unknown",
        _ck("app.minion"),
        serialize(dict(unknown_payload)),
    )
    ctxs = await store._mn_get_all_decoded_contexts()

    assert all(c.workflow_id != "wf-unknown" for c in ctxs)
    assert logger.has_log("StateStore failed to decode stored workflow context")
