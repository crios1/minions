import os
import time
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    serialize_workflow_context,
)
from minions._internal._framework.state_store import StateStore
from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.state_store_inmemory import InMemoryStateStore


pytestmark = pytest.mark.asyncio


@pytest_asyncio.fixture(params=["inmemory", "sqlite"])
async def store_and_logger(
    request: pytest.FixtureRequest, tmp_path: str
) -> AsyncGenerator[tuple[StateStore, InMemoryLogger], None]:
    logger = InMemoryLogger()

    if request.param == "sqlite":
        db_path = os.path.join(tmp_path, "state.db")
        store = SQLiteStateStore(db_path=db_path, logger=logger)
        await store.startup()
        try:
            yield store, logger
        finally:
            await store.shutdown()
        return

    store = InMemoryStateStore(logger=logger)
    yield store, logger


async def test_state_store_contract_roundtrips_runtime_context(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    expected = MinionWorkflowContext(
        minion_modpath="app.minion",
        workflow_id="wf-roundtrip",
        event={"i": 1},
        context={"k": "v"},
        context_cls=dict,
        next_step_index=3,
        started_at=time.time(),
        error_msg=None,
    )

    await store._save_context(expected)
    rows = await store._get_all_contexts()

    assert len(rows) == 1
    actual = rows[0]
    assert actual.workflow_id == expected.workflow_id
    assert actual.minion_modpath == expected.minion_modpath
    assert actual.event == expected.event
    assert actual.context == expected.context
    assert actual.context_cls is dict
    assert actual.next_step_index == expected.next_step_index
    assert actual.error_msg == expected.error_msg


async def test_state_store_contract_starts_empty(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger

    rows = await store._get_all_contexts()

    assert rows == []


async def test_state_store_contract_overwrites_existing_workflow_context(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    workflow_id = "wf-overwrite"
    original = MinionWorkflowContext(
        minion_modpath="app.minion",
        workflow_id=workflow_id,
        event={"i": 1},
        context={"k": "original"},
        context_cls=dict,
        next_step_index=1,
        started_at=time.time(),
        error_msg=None,
    )
    updated = MinionWorkflowContext(
        minion_modpath="app.minion.updated",
        workflow_id=workflow_id,
        event={"i": 2},
        context={"k": "updated"},
        context_cls=dict,
        next_step_index=4,
        started_at=time.time(),
        error_msg="updated-error",
    )

    await store._save_context(original)
    await store._save_context(updated)
    rows = await store._get_all_contexts()

    assert len(rows) == 1
    actual = rows[0]
    assert actual.workflow_id == workflow_id
    assert actual.minion_modpath == updated.minion_modpath
    assert actual.event == updated.event
    assert actual.context == updated.context
    assert actual.context_cls is dict
    assert actual.next_step_index == updated.next_step_index
    assert actual.error_msg == updated.error_msg


async def test_state_store_contract_deletes_existing_workflow_context(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    workflow_id = "wf-delete"
    context = MinionWorkflowContext(
        minion_modpath="app.minion",
        workflow_id=workflow_id,
        event={"i": 1},
        context={"k": "v"},
        context_cls=dict,
        next_step_index=2,
        started_at=time.time(),
        error_msg=None,
    )

    await store._save_context(context)
    await store._delete_context(workflow_id)
    rows = await store._get_all_contexts()

    assert all(row.workflow_id != workflow_id for row in rows)


async def test_state_store_contract_delete_missing_workflow_context_is_noop(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger

    await store._delete_context("wf-missing")
    rows = await store._get_all_contexts()

    assert rows == []


async def test_state_store_contract_save_context_snapshots_payload_at_call_time(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    
    event = {"i": 1}
    context = {"nested": {"value": "original"}}

    payload = serialize_workflow_context(
        MinionWorkflowContext(
            minion_modpath="app.minion",
            workflow_id="wf-snapshot",
            event=event,
            context=context,
            context_cls=dict,
            next_step_index=2,
            started_at=time.time(),
            error_msg=None,
        )
    )

    await store.save_context("wf-snapshot", payload)
    event["i"] = 999
    context["nested"]["value"] = "mutated"
    rows = await store._get_all_contexts()

    assert len(rows) == 1
    actual = rows[0]
    assert actual.workflow_id == "wf-snapshot"
    assert actual.event == {"i": 1}
    assert actual.context == {"nested": {"value": "original"}}


async def test_state_store_contract_get_all_contexts_returns_read_isolated_payloads(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    payload = serialize_workflow_context(
        MinionWorkflowContext(
            minion_modpath="app.minion",
            workflow_id="wf-read-isolation",
            event={"i": 1},
            context={"nested": {"value": "original"}},
            context_cls=dict,
            next_step_index=0,
            started_at=time.time(),
            error_msg=None,
        )
    )

    await store.save_context("wf-read-isolation", payload)
    rows = await store.get_all_contexts()
    rows[0]["context"] = {"nested": {"value": "mutated-after-read"}}

    hydrated = await store._get_all_contexts()
    actual = next(r for r in hydrated if r.workflow_id == "wf-read-isolation")
    assert actual.context == {"nested": {"value": "original"}}


async def test_state_store_contract_get_contexts_for_minion_filters_by_modpath(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    target_a = MinionWorkflowContext(
        minion_modpath="app.minion.target",
        workflow_id="wf-target-a",
        event={"i": 1},
        context={"k": "a"},
        context_cls=dict,
        next_step_index=0,
        started_at=time.time(),
        error_msg=None,
    )
    target_b = MinionWorkflowContext(
        minion_modpath="app.minion.target",
        workflow_id="wf-target-b",
        event={"i": 2},
        context={"k": "b"},
        context_cls=dict,
        next_step_index=1,
        started_at=time.time(),
        error_msg=None,
    )
    other = MinionWorkflowContext(
        minion_modpath="app.minion.other",
        workflow_id="wf-other",
        event={"i": 3},
        context={"k": "c"},
        context_cls=dict,
        next_step_index=2,
        started_at=time.time(),
        error_msg=None,
    )

    await store._save_context(target_a)
    await store._save_context(target_b)
    await store._save_context(other)

    filtered = await store._get_contexts_for_minion("app.minion.target")
    filtered_ids = {ctx.workflow_id for ctx in filtered}
    assert filtered_ids == {"wf-target-a", "wf-target-b"}
    assert all(ctx.minion_modpath == "app.minion.target" for ctx in filtered)

    missing = await store._get_contexts_for_minion("app.minion.missing")
    assert missing == []


async def test_state_store_contract_migrates_v1_unversioned_payload(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, _ = store_and_logger
    legacy_payload = serialize_workflow_context(
        MinionWorkflowContext(
            minion_modpath="app.minion",
            workflow_id="wf-legacy-v1",
            event={"i": 99},
            context={"p": "legacy"},
            context_cls=dict,
            next_step_index=1,
            started_at=time.time(),
            error_msg=None,
        )
    )
    legacy_payload["step_index"] = legacy_payload.pop("next_step_index")
    legacy_payload.pop("schema_version", None)

    await store.save_context("wf-legacy-v1", legacy_payload)
    rows = await store._get_all_contexts()

    row = next(r for r in rows if r.workflow_id == "wf-legacy-v1")
    assert row.next_step_index == 1


async def test_state_store_contract_skips_unknown_schema_payload(
    store_and_logger: tuple[StateStore, InMemoryLogger],
):
    store, logger = store_and_logger
    unknown_payload = serialize_workflow_context(
        MinionWorkflowContext(
            minion_modpath="app.minion",
            workflow_id="wf-unknown",
            event={"i": 100},
            context={"p": "unknown"},
            context_cls=dict,
            next_step_index=0,
            started_at=time.time(),
            error_msg=None,
        )
    )
    unknown_payload["schema_version"] = 999

    await store.save_context("wf-unknown", unknown_payload)
    rows = await store._get_all_contexts()

    assert all(r.workflow_id != "wf-unknown" for r in rows)
    assert logger.has_log("load failed")
