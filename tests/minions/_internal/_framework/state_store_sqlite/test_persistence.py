import sqlite3
from typing import Any

import pytest

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
)
from minions._internal._framework.state_store_sqlite import SQL_WORKFLOWS_SELECT_FOR_ORCHESTRATION
from tests.minions._internal._framework.state_store_sqlite._support import blob_for, mk_ctx
from tests.minions._internal._framework.state_store_sqlite.conftest import MakeStateStoreAndLogger

pytestmark = pytest.mark.asyncio


async def test_save_context_persists_before_returning(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    ctx = mk_ctx(i=1, size=16)
    expected_blob = blob_for(ctx)

    await s.save_context(ctx.workflow_id, ctx.orchestration_id, expected_blob)

    with sqlite3.connect(s.db_path) as conn:
        row = conn.execute(
            "SELECT orchestration_id, context FROM workflows WHERE workflow_id = ?",
            (ctx.workflow_id,),
        ).fetchone()

    assert row == (ctx.orchestration_id, expected_blob)


async def test_framework_serialized_context_remains_hydratable(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    ctx = mk_ctx(i=77, size=16)

    result = await s._mn_serialize_and_save_context(ctx)
    rows = await s.get_all_contexts()

    assert result.persisted
    assert result.failure_stage is None
    assert result.error is None
    assert not result.retryable
    row = next(r for r in rows if r.workflow_id == ctx.workflow_id)
    assert row.orchestration_id == ctx.orchestration_id
    assert deserialize_workflow_context_blob(row.context) == ctx


async def test_get_contexts_for_orchestration_uses_orchestration_index(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, _ = await make_state_store_and_logger()
    target = mk_ctx(i=1, size=16)
    other = mk_ctx(i=2, size=16)

    await s.save_context(target.workflow_id, target.orchestration_id, blob_for(target))
    await s.save_context(other.workflow_id, other.orchestration_id, blob_for(other))

    rows = await s.get_contexts_for_orchestration(target.orchestration_id)

    assert [(row.workflow_id, row.orchestration_id) for row in rows] == [
        (target.workflow_id, target.orchestration_id),
    ]

    assert s._db is not None
    async with s._db.execute(
        f"EXPLAIN QUERY PLAN {SQL_WORKFLOWS_SELECT_FOR_ORCHESTRATION}",
        (target.orchestration_id,),
    ) as cursor:
        plan_rows = await cursor.fetchall()

    plan = " ".join(str(column) for row in plan_rows for column in row)

    assert "SEARCH workflows" in plan
    assert "orchestration_id=?" in plan
    assert "SCAN workflows" not in plan


async def test_runtime_save_context_serialize_failure_is_non_retryable_and_logged(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch: pytest.MonkeyPatch,
):
    s, logger = await make_state_store_and_logger()

    def _boom(_ctx: MinionWorkflowContext[Any, Any]) -> bytes:
        raise ValueError("serialize boom")

    monkeypatch.setattr(
        "minions._internal._framework.state_store.serialize_persisted_workflow_context",
        _boom,
    )

    ctx = mk_ctx(i=55, size=16)
    result = await s._mn_serialize_and_save_context(ctx)
    rows = await s.get_all_contexts()

    assert not result.persisted
    assert result.failure_stage == "serialize"
    assert isinstance(result.error, ValueError)
    assert not result.retryable
    assert all(row.workflow_id != ctx.workflow_id for row in rows)
    assert await logger.wait_for_log("StateStore failed to serialize workflow context")


async def test_runtime_save_context_failure_returns_structured_result(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch: pytest.MonkeyPatch,
):
    s, logger = await make_state_store_and_logger()

    async def _boom(*_args: object) -> None:
        raise RuntimeError("save boom")

    monkeypatch.setattr(s, "save_context", _boom)

    ctx = mk_ctx(i=56, size=16)
    result = await s._mn_serialize_and_save_context(ctx)

    assert not result.persisted
    assert result.failure_stage == "save"
    assert isinstance(result.error, RuntimeError)
    assert result.retryable
    assert await logger.wait_for_log("save_context failed")
