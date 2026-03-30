import asyncio
import os
import time
from collections.abc import AsyncGenerator
import pytest
import pytest_asyncio

from tests.assets.support.logger_inmemory import InMemoryLogger
from minions._internal._framework.minion_workflow_context_codec import (
    serialize_workflow_context,
)
from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext

pytestmark = pytest.mark.asyncio

def mk_ctx(i=0, size=32, modpath="app.minion"):
    return MinionWorkflowContext(
        minion_modpath=modpath,
        workflow_id=f"wf-{i}",
        event={"i": i},
        context={"p": "x" * size},
        context_cls=dict,
        next_step_index=i,
        started_at=time.time(),
        error_msg=None,
    )


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

async def test_startup_creates_table_and_calibrates(store_and_logger):
    s, logger = store_and_logger
    assert s._db is not None
    assert s._batch_max_n is not None and s._batch_max_ms is not None
    assert s._page_size >= 1024  # sanity: page size discovered
    # sanity: calibration log emitted with structured fields
    assert any("p50_commit_ms" in log.kwargs for log in logger.logs)

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
    # SQLite reports PRAGMA synchronous as an integer value.
    # OFF=0, NORMAL=1, FULL=2, EXTRA=3
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
    c1 = mk_ctx(i=1, size=16)
    await s.save_context(c1.workflow_id, serialize_workflow_context(c1))
    ctxs = await s.get_all_contexts()

    assert any(c["workflow_id"] == c1.workflow_id for c in ctxs)


async def test_save_context_serialize_failure_is_fail_open_and_logged(store_and_logger, monkeypatch):
    s, logger = store_and_logger

    def _boom(_payload):
        raise ValueError("serialize boom")

    monkeypatch.setattr(
        "minions._internal._framework.state_store_sqlite.serialize",
        _boom,
    )

    ctx = mk_ctx(i=55, size=16)
    await s.save_context(ctx.workflow_id, serialize_workflow_context(ctx))
    ctxs = await s.get_all_contexts()

    assert all(c["workflow_id"] != ctx.workflow_id for c in ctxs)
    assert await logger.wait_for_log("save_context_payload serialize failed")


async def test_save_context_batches_but_waits_for_batch_commit(store_and_logger):
    s, _ = store_and_logger
    s._batch_max_n = 100
    s._batch_max_ms = 50

    c1 = mk_ctx(i=1, size=16)
    c2 = mk_ctx(i=2, size=24)
    
    t1 = asyncio.create_task(s.save_context(c1.workflow_id, serialize_workflow_context(c1)))
    await asyncio.sleep(0)
    assert len(s._batch) == 1
    assert not t1.done()

    t2 = asyncio.create_task(s.save_context(c2.workflow_id, serialize_workflow_context(c2)))
    await asyncio.sleep(0)
    assert len(s._batch) == 2
    assert not t1.done()
    assert not t2.done()

    await asyncio.gather(t1, t2)
    assert len(s._batch) == 0
    ctxs = await s.get_all_contexts()

    ids = {c["workflow_id"] for c in ctxs}
    assert ids == {c1.workflow_id, c2.workflow_id}


async def test_flush_on_batch_cap(store_and_logger):
    s, _ = store_and_logger
    s._batch_max_ms = 10_000
    s._batch_max_n = 3
    tasks = []
    for i in range(3):
        ctx = mk_ctx(i=i)
        tasks.append(asyncio.create_task(s.save_context(ctx.workflow_id, serialize_workflow_context(ctx))))
    await asyncio.gather(*tasks)

    assert len(s._batch) == 0
    ctxs = await s.get_all_contexts()
    assert len(ctxs) >= 3

async def test_large_state_warning(store_and_logger):
    s, logger = store_and_logger
    s._size_warn_pages = 1  # make easy to trigger
    s._page_size = 4096
    big = mk_ctx(i=10, size=8192)  # > 1 page after serialization overhead
    await s.save_context(big.workflow_id, serialize_workflow_context(big))
    assert await logger.wait_for_log("Large MinionWorkflowContext"), "expected large-state warning"

async def test_commit_metrics_populated(store_and_logger):
    s, _ = store_and_logger
    for i in range(5):
        ctx = mk_ctx(i=i)
        await s.save_context(ctx.workflow_id, serialize_workflow_context(ctx))

    assert s._commit_ms_hist, "expected commit timings recorded"

async def test_backlog_warning(store_and_logger):
    s, logger = store_and_logger
    s._warn_cfg["backlog_n"] = (2, 999)
    s._batch_max_n = 100
    s._batch_max_ms = 10_000
    tasks = []
    for i in range(1, 4):
        ctx = mk_ctx(i=i)
        tasks.append(asyncio.create_task(s.save_context(ctx.workflow_id, serialize_workflow_context(ctx))))
    assert await logger.wait_for_log("backlog>"), "expected backlog warning"
    await s._flush()
    await asyncio.gather(*tasks)


async def test_shutdown_flushes_pending_batch(store_and_logger):
    s, _ = store_and_logger
    s._batch_max_ms = 10_000
    ctx = mk_ctx(i=42)
    save_task = asyncio.create_task(s.save_context(ctx.workflow_id, serialize_workflow_context(ctx)))
    await asyncio.sleep(0)
    assert s._batch
    assert not save_task.done()
    await s.shutdown()
    await save_task
    # re-open to read
    logger2 = InMemoryLogger()
    s2 = SQLiteStateStore(db_path=s.db_path, logger=logger2)
    await s2.startup()
    ctxs = await s2.get_all_contexts()
    await s2.shutdown()
    assert any(c["workflow_id"] == "wf-42" for c in ctxs)

async def test_commit_p95_warning(store_and_logger):
    s, logger = store_and_logger
    s._warn_cfg["commit_p95_ms"] = (5.0, 9999.0)  # trip warn easily
    # Keep throughput warnings effectively disabled so this test isolates commit p95.
    s._warn_cfg["rows_per_sec"] = (float("inf"), float("inf"))

    # Seed enough commit samples for p95 and force the threshold crossing.
    s._commit_ms_hist.extend([10.0] * 22)  # >=20 so p95 is meaningful

    s._check_perf_signals()
    assert await logger.wait_for_log("commit_p95=", timeout=1.0), "expected commit_p95 warning to fire"
