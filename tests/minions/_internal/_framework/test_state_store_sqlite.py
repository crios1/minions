import asyncio
import os
import time
import pytest
import pytest_asyncio

from tests.assets.support.logger_inmemory import InMemoryLogger
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
        step_index=i,
        started_at=time.time(),
        error_msg=None,
    )

async def wait_for_log(logger: InMemoryLogger, substr: str, timeout=0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if logger.has_log(substr):
            return True
        await asyncio.sleep(0)  # let scheduled tasks run
    return False

@pytest_asyncio.fixture
async def store(tmp_path):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(db_path=db_path, logger=logger)
    await s.startup()
    try:
        yield s, logger
    finally:
        await s.shutdown()

async def wait_flush(s: SQLiteStateStore, timeout=0.5):
    t = s._flush_task
    if not t:
        return
    try:
        await asyncio.wait_for(asyncio.shield(t), timeout)
    except asyncio.TimeoutError:
        pass  # flush is debounced; tests that need it should force size-cap

async def test_startup_creates_table_and_calibrates(store):
    s, logger = store
    assert s._db is not None
    assert s._batch_max_n is not None and s._batch_max_ms is not None
    assert s._page_size >= 1024  # sanity: page size discovered
    # sanity: calibration log emitted with structured fields
    assert any("p50_commit_ms" in log.kwargs for log in logger.logs)

async def test_save_coalesces_and_debounce(store):
    s, logger = store
    s._batch_max_n = 100
    s._batch_max_ms = 50
    c1 = mk_ctx(i=1, size=16)
    await s.save_context(c1)
    assert len(s._batch) == 1
    # coalesce same workflow_id
    c1b = mk_ctx(i=1, size=24)
    await s.save_context(c1b)
    assert len(s._batch) == 1
    # trigger timer-based flush without sleeping: force size-cap instead
    s._batch_max_n = 1
    await s.save_context(mk_ctx(i=2))
    await wait_flush(s)
    assert len(s._batch) == 0
    rows = await s.get_all_contexts()
    ids = {r.workflow_id for r in rows}
    assert {"wf-1", "wf-2"}.issubset(ids)

async def test_flush_on_batch_cap(store):
    s, _ = store
    s._batch_max_ms = 10_000
    s._batch_max_n = 3
    for i in range(3):
        await s.save_context(mk_ctx(i=i))
    # immediate flush because size cap
    assert len(s._batch) == 0
    rows = await s.get_all_contexts()
    assert len(rows) >= 3

async def test_large_state_warning(store):
    s, logger = store
    s._size_warn_pages = 1  # make easy to trigger
    s._page_size = 4096
    big = mk_ctx(i=10, size=8192)  # > 1 page after serialization overhead
    await s.save_context(big)
    await wait_flush(s)
    assert logger.has_log("Large MinionWorkflowContext"), "expected large-state warning"

async def test_backlog_warning(store):
    s, logger = store
    s._warn_cfg["backlog_n"] = (2, 999)
    s._batch_max_n = 100
    s._batch_max_ms = 10_000
    await s.save_context(mk_ctx(i=1))
    await s.save_context(mk_ctx(i=2))
    await s.save_context(mk_ctx(i=3))  # backlog now 3 -> warn
    assert await wait_for_log(logger, "backlog>"), "expected backlog warning"

async def test_commit_metrics_populated(store):
    s, _ = store
    s._batch_max_n = 1
    for i in range(5):
        await s.save_context(mk_ctx(i=i))
    await wait_flush(s)
    assert s._commit_ms_hist, "expected commit timings recorded"

async def test_shutdown_flushes(store):
    s, _ = store
    s._batch_max_ms = 10_000
    await s.save_context(mk_ctx(i=42))
    assert s._batch  # not flushed yet
    await s.shutdown()
    # re-open to read
    logger2 = InMemoryLogger()
    s2 = SQLiteStateStore(db_path=s.db_path, logger=logger2)
    await s2.startup()
    rows = await s2.get_all_contexts()
    await s2.shutdown()
    assert any(r.workflow_id == "wf-42" for r in rows)

async def test_delete_context_removes_row(store):
    s, _ = store
    c = mk_ctx(i=77)
    await s.save_context(c)
    await wait_flush(s)
    rows = await s.get_all_contexts()
    assert any(r.workflow_id == "wf-77" for r in rows)

    await s.delete_context("wf-77")
    rows2 = await s.get_all_contexts()
    assert all(r.workflow_id != "wf-77" for r in rows2)

async def test_commit_p95_warning(store, monkeypatch):
    s, logger = store
    s._batch_max_n = 1
    s._warn_cfg["commit_p95_ms"] = (5.0, 9999.0)  # trip warn easily

    # Inject latency inside the timed window of _upsert_now by slowing commit()
    orig_commit = s._db.commit
    async def delayed_commit():
        await asyncio.sleep(0.05)  # inflate measured commit duration
        return await orig_commit()
    monkeypatch.setattr(s._db, "commit", delayed_commit)

    # Generate enough samples for p95 computation
    for i in range(22):  # >=20 so p95 is meaningful
        await s.save_context(mk_ctx(i=2000 + i))
        await wait_flush(s)

    # Give the scheduled warning task a moment to run
    assert await wait_for_log(logger, "commit_p95=", timeout=1.0), "expected commit_p95 warning to fire"

