import sqlite3

import pytest

from minions._internal._framework.minion_workflow_context_codec import deserialize_workflow_context_blob
from tests.minions._internal._framework.state_store_sqlite._support import blob_for, mk_ctx
from tests.minions._internal._framework.state_store_sqlite.conftest import MakeStateStoreAndLogger


pytestmark = pytest.mark.asyncio


async def test_save_context_persists_before_returning(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    ctx = mk_ctx(i=1, size=16)
    expected_blob = blob_for(ctx)

    await s.save_context(ctx.workflow_id, ctx.minion_composite_key, expected_blob)

    with sqlite3.connect(s.db_path) as conn:
        row = conn.execute(
            "SELECT orchestration_id, context FROM workflows WHERE workflow_id = ?",
            (ctx.workflow_id,),
        ).fetchone()

    assert row == (ctx.minion_composite_key, expected_blob)


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
    assert row.orchestration_id == ctx.minion_composite_key
    assert deserialize_workflow_context_blob(row.context) == ctx


async def test_runtime_save_context_serialize_failure_is_non_retryable_and_logged(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, logger = await make_state_store_and_logger()

    def _boom(_ctx):
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
    monkeypatch,
):
    s, logger = await make_state_store_and_logger()

    async def _boom(*_args):
        raise RuntimeError("save boom")

    monkeypatch.setattr(s, "save_context", _boom)

    ctx = mk_ctx(i=56, size=16)
    result = await s._mn_serialize_and_save_context(ctx)

    assert not result.persisted
    assert result.failure_stage == "save"
    assert isinstance(result.error, RuntimeError)
    assert result.retryable
    assert await logger.wait_for_log("save_context failed")
