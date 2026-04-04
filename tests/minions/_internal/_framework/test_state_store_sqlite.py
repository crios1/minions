import asyncio
import os
import time
from collections.abc import AsyncGenerator

import pytest
import pytest_asyncio

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
    persist_workflow_context,
)
from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from minions._internal._utils.serialization import serialize
from tests.assets.support.logger_inmemory import InMemoryLogger


pytestmark = pytest.mark.asyncio


def mk_ctx(i=0, size=32, modpath="app.minion"):
    return MinionWorkflowContext(
        minion_composite_key=f"{modpath}|cfg-{i}|app.pipeline",
        minion_modpath=modpath,
        workflow_id=f"wf-{i}",
        event={"i": i},
        context={"p": "x" * size},
        context_cls=dict,
        next_step_index=i,
        started_at=time.time(),
        error_msg=None,
    )


def _blob_for(ctx: MinionWorkflowContext) -> bytes:
    return serialize(persist_workflow_context(ctx))


@pytest_asyncio.fixture
async def store_and_logger(
    tmp_path,
) -> AsyncGenerator[tuple[SQLiteStateStore, InMemoryLogger], None]:
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(db_path=db_path, logger=logger)
    await s.startup()
    try:
        yield s, logger
    finally:
        await s.shutdown()


async def test_startup_creates_table_index_and_calibrates(store_and_logger):
    s, logger = store_and_logger
    assert s._db is not None
    assert s._batch_max_n is not None and s._batch_max_ms is not None
    assert s._page_size >= 1024
    assert any("p50_commit_ms" in log.kwargs for log in logger.logs)

    async with s._db.execute("PRAGMA index_list(workflows)") as c:
        rows = await c.fetchall()
    assert any(row[1] == "idx_workflows_orchestration_id" for row in rows)


async def test_startup_applies_configured_wal_autocheckpoint(tmp_path):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    configured_pages = 2048
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        wal_autocheckpoint=configured_pages,
    )
    await s.startup()
    try:
        assert s._db is not None
        async with s._db.execute("PRAGMA wal_autocheckpoint") as c:
            row = await c.fetchone()
        assert row is not None
        assert int(row[0]) == configured_pages
    finally:
        await s.shutdown()


async def test_startup_applies_configured_busy_timeout(tmp_path):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    configured_ms = 7500
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        busy_timeout_ms=configured_ms,
    )
    await s.startup()
    try:
        assert s._db is not None
        async with s._db.execute("PRAGMA busy_timeout") as c:
            row = await c.fetchone()
        assert row is not None
        assert int(row[0]) == configured_ms
    finally:
        await s.shutdown()


async def test_startup_applies_configured_journal_mode(tmp_path):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    configured_mode = "DELETE"
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        journal_mode=configured_mode,
    )
    await s.startup()
    try:
        assert s._db is not None
        async with s._db.execute("PRAGMA journal_mode") as c:
            row = await c.fetchone()
        assert row is not None
        assert str(row[0]).upper() == configured_mode
    finally:
        await s.shutdown()


async def test_startup_applies_configured_synchronous(tmp_path):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        synchronous="OFF",
    )
    await s.startup()
    try:
        assert s._db is not None
        async with s._db.execute("PRAGMA synchronous") as c:
            row = await c.fetchone()
        assert row is not None
        assert int(row[0]) == 0
    finally:
        await s.shutdown()


async def test_save_context_persists_before_returning(store_and_logger):
    s, _ = store_and_logger
    ctx = mk_ctx(i=1, size=16)
    await s.save_context(ctx.workflow_id, ctx.minion_composite_key, _blob_for(ctx))
    rows = await s.get_all_contexts()

    assert any(row.workflow_id == ctx.workflow_id for row in rows)


async def test_runtime_blob_path_remains_readable_via_framework_hydration(store_and_logger):
    s, _ = store_and_logger
    ctx = mk_ctx(i=77, size=16)

    await s._mn_serialize_and_save_context(ctx)
    rows = await s.get_all_contexts()

    row = next(r for r in rows if r.workflow_id == ctx.workflow_id)
    assert row.orchestration_id == ctx.minion_composite_key
    assert deserialize_workflow_context_blob(row.context) == ctx


async def test_runtime_save_context_serialize_failure_is_fail_open_and_logged(
    store_and_logger,
    monkeypatch,
):
    s, logger = store_and_logger

    def _boom(_ctx):
        raise ValueError("serialize boom")

    monkeypatch.setattr(
        "minions._internal._framework.state_store.serialize_persisted_workflow_context",
        _boom,
    )

    ctx = mk_ctx(i=55, size=16)
    await s._mn_serialize_and_save_context(ctx)
    rows = await s.get_all_contexts()

    assert all(row.workflow_id != ctx.workflow_id for row in rows)
    assert await logger.wait_for_log("StateStore failed to serialize workflow context")


async def test_save_context_batches_but_waits_for_batch_commit(store_and_logger):
    s, _ = store_and_logger
    s._batch_max_n = 100
    s._batch_max_ms = 10_000

    c1 = mk_ctx(i=1, size=16)
    c2 = mk_ctx(i=2, size=24)

    t1 = asyncio.create_task(s.save_context(c1.workflow_id, c1.minion_composite_key, _blob_for(c1)))
    t2 = asyncio.create_task(s.save_context(c2.workflow_id, c2.minion_composite_key, _blob_for(c2)))

    deadline = asyncio.get_running_loop().time() + 1.0
    expected_ids = {c1.workflow_id, c2.workflow_id}
    while True:
        batched_ids = {workflow_id for workflow_id, *_rest in s._batch}
        if batched_ids == expected_ids:
            break
        if asyncio.get_running_loop().time() >= deadline:
            raise AssertionError(f"timed out waiting for batched ids {expected_ids!r}; saw {batched_ids!r}")
        await asyncio.sleep(0.001)

    assert not t1.done()
    assert not t2.done()

    await s._flush()
    await asyncio.gather(t1, t2)
    assert len(s._batch) == 0
    rows = await s.get_all_contexts()

    ids = {row.workflow_id for row in rows}
    assert ids == {c1.workflow_id, c2.workflow_id}


async def test_flush_paths_do_not_overlap_transactions(store_and_logger, monkeypatch):
    s, _ = store_and_logger
    s._batch_max_n = 100
    s._batch_max_ms = 1

    first_commit_entered = asyncio.Event()
    allow_first_commit = asyncio.Event()
    active_commits = 0
    max_active_commits = 0

    original_commit_batch_now = s._commit_batch_now

    async def wrapped_commit_batch_now(items):
        nonlocal active_commits, max_active_commits
        active_commits += 1
        max_active_commits = max(max_active_commits, active_commits)
        try:
            if not first_commit_entered.is_set():
                first_commit_entered.set()
                await allow_first_commit.wait()
            return await original_commit_batch_now(items)
        finally:
            active_commits -= 1

    monkeypatch.setattr(s, "_commit_batch_now", wrapped_commit_batch_now)

    ctx1 = mk_ctx(i=1, size=16)
    ctx2 = mk_ctx(i=2, size=24)

    save1 = asyncio.create_task(s.save_context(ctx1.workflow_id, ctx1.minion_composite_key, _blob_for(ctx1)))
    await asyncio.wait_for(first_commit_entered.wait(), timeout=1.0)

    save2 = asyncio.create_task(s.save_context(ctx2.workflow_id, ctx2.minion_composite_key, _blob_for(ctx2)))
    await asyncio.sleep(0)
    read_all = asyncio.create_task(s.get_all_contexts())
    await asyncio.sleep(0)

    assert not save1.done()
    assert not save2.done()
    assert not read_all.done()
    assert max_active_commits == 1

    allow_first_commit.set()
    rows = await asyncio.wait_for(read_all, timeout=2.0)
    await asyncio.wait_for(asyncio.gather(save1, save2), timeout=2.0)

    ids = {row.workflow_id for row in rows}
    assert ids == {ctx1.workflow_id, ctx2.workflow_id}
    assert max_active_commits == 1


async def test_flush_on_batch_cap(store_and_logger):
    s, _ = store_and_logger
    s._batch_max_ms = 10_000
    s._batch_max_n = 3
    tasks = []
    for i in range(3):
        ctx = mk_ctx(i=i)
        tasks.append(
            asyncio.create_task(
                s.save_context(ctx.workflow_id, ctx.minion_composite_key, _blob_for(ctx))
            )
        )
    await asyncio.gather(*tasks)

    assert len(s._batch) == 0
    rows = await s.get_all_contexts()
    assert len(rows) >= 3


async def test_large_state_warning(store_and_logger):
    s, logger = store_and_logger
    s._size_warn_pages = 1
    s._page_size = 4096
    big = mk_ctx(i=10, size=8192)
    await s.save_context(big.workflow_id, big.minion_composite_key, _blob_for(big))
    assert await logger.wait_for_log("Large MinionWorkflowContext"), "expected large-state warning"


async def test_commit_metrics_populated(store_and_logger):
    s, _ = store_and_logger
    for i in range(5):
        ctx = mk_ctx(i=i)
        await s.save_context(ctx.workflow_id, ctx.minion_composite_key, _blob_for(ctx))

    assert s._commit_ms_hist, "expected commit timings recorded"


async def test_backlog_warning(store_and_logger):
    s, logger = store_and_logger
    s._warn_cfg["backlog_n"] = (2, 999)
    s._batch_max_n = 100
    s._batch_max_ms = 10_000
    tasks = []
    for i in range(1, 4):
        ctx = mk_ctx(i=i)
        tasks.append(
            asyncio.create_task(
                s.save_context(ctx.workflow_id, ctx.minion_composite_key, _blob_for(ctx))
            )
        )
    assert await logger.wait_for_log("backlog>"), "expected backlog warning"
    await s._flush()
    await asyncio.gather(*tasks)


async def test_shutdown_flushes_pending_batch(store_and_logger):
    s, _ = store_and_logger
    s._batch_max_ms = 10_000
    ctx = mk_ctx(i=42)
    save_task = asyncio.create_task(
        s.save_context(ctx.workflow_id, ctx.minion_composite_key, _blob_for(ctx))
    )
    await asyncio.sleep(0)
    assert s._batch
    assert not save_task.done()
    await s.shutdown()
    await save_task

    logger2 = InMemoryLogger()
    s2 = SQLiteStateStore(db_path=s.db_path, logger=logger2)
    await s2.startup()
    rows = await s2.get_all_contexts()
    await s2.shutdown()
    assert any(row.workflow_id == "wf-42" for row in rows)


async def test_commit_p95_warning(store_and_logger):
    s, logger = store_and_logger
    s._warn_cfg["commit_p95_ms"] = (5.0, 9999.0)
    s._warn_cfg["rows_per_sec"] = (float("inf"), float("inf"))

    s._commit_ms_hist.extend([10.0] * 22)

    s._check_perf_signals()
    assert await logger.wait_for_log("commit_p95=", timeout=1.0), "expected commit_p95 warning to fire"
