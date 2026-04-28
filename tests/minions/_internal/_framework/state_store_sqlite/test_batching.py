import asyncio
import sqlite3

import pytest

from minions._internal._framework.minion_workflow_context_codec import deserialize_workflow_context_blob
from minions._internal._framework.state_store_sqlite import (
    PendingWrite,
    SQL_WORKFLOW_UPSERT,
    SQLiteStateStore,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.minions._internal._framework.state_store_sqlite._support import blob_for, mk_ctx
from tests.minions._internal._framework.state_store_sqlite.conftest import MakeStateStoreAndLogger


pytestmark = pytest.mark.asyncio


async def _cleanup_tasks(
    *tasks: asyncio.Task | None,
) -> None:
    active_tasks = [task for task in tasks if task is not None]
    if not active_tasks:
        return

    _done, pending = await asyncio.wait(active_tasks, timeout=2.0)
    for task in pending:
        task.cancel()
    await asyncio.gather(*active_tasks, return_exceptions=True)


class _BlockedCommitGate:
    def __init__(self, s: SQLiteStateStore, monkeypatch: pytest.MonkeyPatch):
        self.entered = asyncio.Event()
        self.commit_count = 0
        self.max_active_commit_count = 0
        self.operation_order: list[tuple[str, str]] = []
        self._active_commit_count = 0
        self._release = asyncio.Event()
        # Wrap the private commit worker to hold a transaction open at a deterministic point.
        original_commit_batch_now = s._commit_batch_now

        async def wrapped_commit_batch_now(items):
            self.commit_count += 1
            self.operation_order.extend((item.op, item.workflow_id) for item in items)
            self._active_commit_count += 1
            self.max_active_commit_count = max(
                self.max_active_commit_count,
                self._active_commit_count,
            )
            try:
                if not self.entered.is_set():
                    self.entered.set()
                    await self._release.wait()
                return await original_commit_batch_now(items)
            finally:
                self._active_commit_count -= 1

        monkeypatch.setattr(s, "_commit_batch_now", wrapped_commit_batch_now)

    async def wait_until_entered(self) -> None:
        await asyncio.wait_for(self.entered.wait(), timeout=1.0)

    def release(self) -> None:
        self._release.set()

    async def release_and_cleanup_tasks(self, *tasks: asyncio.Task | None) -> None:
        self.release()
        await _cleanup_tasks(*tasks)


async def test_save_context_batches_but_waits_for_batch_commit(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger(batch_max_queued_writes=2)

    # prevent the timer flush from racing the second write that triggers the batch cap
    s._batch_max_flush_delay_ms = 3_000

    c1 = mk_ctx(i=1, size=16)
    c2 = mk_ctx(i=2, size=24)

    t1 = asyncio.create_task(s.save_context(c1.workflow_id, c1.minion_composite_key, blob_for(c1)))
    await asyncio.sleep(0)
    assert not t1.done()

    t2 = asyncio.create_task(s.save_context(c2.workflow_id, c2.minion_composite_key, blob_for(c2)))

    await asyncio.gather(t1, t2)
    rows = await s.get_all_contexts()

    ids = {row.workflow_id for row in rows}
    assert ids == {c1.workflow_id, c2.workflow_id}


async def test_immediate_batch_mode_commits_without_scheduling_flush_timer(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=1,
        batch_max_flush_delay_ms=40,
    )
    ctx = mk_ctx(i=302, size=16)

    await s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))

    assert len(s._batch_buffer) == 0
    assert s._batch_buffer_flush_task is None
    rows = await s.get_all_contexts()
    assert any(row.workflow_id == ctx.workflow_id for row in rows)


async def test_batch_max_interarrival_delay_ms_none_preserves_scheduled_flush_behavior(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=40,
        batch_max_interarrival_delay_ms=None,
    )
    ctx = mk_ctx(i=701, size=16)
    save_task = asyncio.create_task(s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx)))

    await asyncio.sleep(0.01)

    assert not save_task.done()
    await s._flush()
    await asyncio.wait_for(save_task, timeout=1.0)


async def test_single_write_flushes_on_batch_max_interarrival_delay_ms(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=40,
        batch_max_interarrival_delay_ms=5,
    )
    ctx = mk_ctx(i=702, size=16)
    started = asyncio.get_running_loop().time()

    await asyncio.wait_for(
        s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx)),
        timeout=0.1,
    )

    assert asyncio.get_running_loop().time() - started < 0.04
    rows = await s.get_all_contexts()
    assert any(row.workflow_id == ctx.workflow_id for row in rows)


async def test_writes_inside_batch_max_interarrival_delay_ms_batch_together(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=40,
        batch_max_interarrival_delay_ms=20,
    )
    commit_row_counts: list[int] = []
    original_commit_batch_now = s._commit_batch_now

    async def wrapped_commit_batch_now(items):
        commit_row_counts.append(len(items))
        return await original_commit_batch_now(items)

    monkeypatch.setattr(s, "_commit_batch_now", wrapped_commit_batch_now)
    tasks = []
    for i in range(3):
        ctx = mk_ctx(i=710 + i, size=16)
        tasks.append(asyncio.create_task(s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))))
        await asyncio.sleep(0.005)

    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1.0)

    assert commit_row_counts == [3]


async def test_max_flush_delay_still_applies_during_continuous_arrivals(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=30,
        batch_max_interarrival_delay_ms=20,
    )
    first_commit = asyncio.Event()
    original_commit_batch_now = s._commit_batch_now

    async def wrapped_commit_batch_now(items):
        first_commit.set()
        return await original_commit_batch_now(items)

    monkeypatch.setattr(s, "_commit_batch_now", wrapped_commit_batch_now)
    tasks = []
    started = asyncio.get_running_loop().time()

    async def produce() -> None:
        for i in range(10):
            ctx = mk_ctx(i=720 + i, size=16)
            tasks.append(asyncio.create_task(s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))))
            await asyncio.sleep(0.005)

    producer = asyncio.create_task(produce())
    await asyncio.wait_for(first_commit.wait(), timeout=0.2)

    assert asyncio.get_running_loop().time() - started < 0.05
    await producer
    await asyncio.wait_for(asyncio.gather(*tasks), timeout=1.0)


async def test_delete_context_batches_but_waits_for_batch_commit(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    ctx = mk_ctx(i=301, size=16)
    # Persist the setup row immediately before switching into batched delete mode.
    s._batch_max_queued_writes = 1
    await s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))

    # Keep the first delete buffered until the second delete reaches the batch cap.
    s._batch_max_queued_writes = 2
    s._batch_max_flush_delay_ms = 3_000

    delete_task = asyncio.create_task(s.delete_context(ctx.workflow_id))
    await asyncio.sleep(0)
    assert not delete_task.done()

    trigger_task = asyncio.create_task(s.delete_context("wf-missing-delete-trigger"))

    await asyncio.gather(delete_task, trigger_task)
    rows = await s.get_all_contexts()

    assert all(row.workflow_id != ctx.workflow_id for row in rows)


async def test_flush_paths_do_not_overlap_transactions(make_state_store_and_logger: MakeStateStoreAndLogger, monkeypatch):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=5,
    )

    commit_gate = _BlockedCommitGate(s, monkeypatch)
    ctx1 = mk_ctx(i=1, size=16)
    ctx2 = mk_ctx(i=2, size=24)
    save1: asyncio.Task | None = None
    save2: asyncio.Task | None = None
    read_all: asyncio.Task | None = None

    try:
        save1 = asyncio.create_task(s.save_context(ctx1.workflow_id, ctx1.minion_composite_key, blob_for(ctx1)))
        await commit_gate.wait_until_entered()

        save2 = asyncio.create_task(s.save_context(ctx2.workflow_id, ctx2.minion_composite_key, blob_for(ctx2)))
        await asyncio.sleep(0)
        read_all = asyncio.create_task(s.get_all_contexts())
        await asyncio.sleep(0)

        assert not save1.done()
        assert not save2.done()
        assert not read_all.done()
        assert commit_gate.max_active_commit_count == 1

        commit_gate.release()
        rows = await asyncio.wait_for(read_all, timeout=2.0)
        await asyncio.wait_for(asyncio.gather(save1, save2), timeout=2.0)

        ids = {row.workflow_id for row in rows}
        assert ids == {ctx1.workflow_id, ctx2.workflow_id}
        assert commit_gate.max_active_commit_count == 1
    finally:
        await commit_gate.release_and_cleanup_tasks(save1, save2, read_all)


async def test_write_arriving_during_scheduled_flush_gets_next_flush_task(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=5,
    )

    commit_gate = _BlockedCommitGate(s, monkeypatch)
    ctx1 = mk_ctx(i=601, size=16)
    ctx2 = mk_ctx(i=602, size=24)
    save1: asyncio.Task | None = None
    save2: asyncio.Task | None = None

    try:
        save1 = asyncio.create_task(s.save_context(ctx1.workflow_id, ctx1.minion_composite_key, blob_for(ctx1)))
        await commit_gate.wait_until_entered()

        save2 = asyncio.create_task(s.save_context(ctx2.workflow_id, ctx2.minion_composite_key, blob_for(ctx2)))
        await asyncio.sleep(0)
        assert not save2.done()

        commit_gate.release()
        await asyncio.wait_for(save1, timeout=1.0)
        await asyncio.wait_for(save2, timeout=1.0)

        rows = await s.get_all_contexts()
        ids = {row.workflow_id for row in rows}
        assert {ctx1.workflow_id, ctx2.workflow_id}.issubset(ids)
        assert commit_gate.commit_count >= 2
    finally:
        await commit_gate.release_and_cleanup_tasks(save1, save2)


async def test_flush_on_batch_cap(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger(batch_max_queued_writes=3)

    # prevent timer flush so only the queued-write cap can commit this batch
    s._batch_max_flush_delay_ms = 10_000

    tasks = []
    for i in range(3):
        ctx = mk_ctx(i=i)
        tasks.append(
            asyncio.create_task(
                s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
            )
        )
    await asyncio.gather(*tasks)

    assert len(s._batch_buffer) == 0
    rows = await s.get_all_contexts()
    assert len(rows) >= 3


async def test_cancelled_cap_triggering_save_does_not_cancel_shared_batch_commit(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(batch_max_queued_writes=2)

    # prevent timer flush so cancellation happens while the cap-triggered commit is shared
    s._batch_max_flush_delay_ms = 3_000

    commit_gate = _BlockedCommitGate(s, monkeypatch)
    c1 = mk_ctx(i=501, size=16)
    c2 = mk_ctx(i=502, size=24)
    save1: asyncio.Task | None = None
    save2: asyncio.Task | None = None

    try:
        save1 = asyncio.create_task(s.save_context(c1.workflow_id, c1.minion_composite_key, blob_for(c1)))
        await asyncio.sleep(0)
        save2 = asyncio.create_task(s.save_context(c2.workflow_id, c2.minion_composite_key, blob_for(c2)))
        await commit_gate.wait_until_entered()

        save2.cancel()
        with pytest.raises(asyncio.CancelledError):
            await save2

        commit_gate.release()
        await asyncio.wait_for(save1, timeout=1.0)

        rows = await s.get_all_contexts()
        ids = {row.workflow_id for row in rows}
        assert {c1.workflow_id, c2.workflow_id}.issubset(ids)
    finally:
        await commit_gate.release_and_cleanup_tasks(save1, save2)


async def test_separate_batches_commit_in_fifo_order_for_same_workflow(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(batch_max_queued_writes=1)

    # prevent timer flush so FIFO ordering is driven by explicit cap-triggered batches
    s._batch_max_flush_delay_ms = 3_000

    commit_gate = _BlockedCommitGate(s, monkeypatch)
    ctx = mk_ctx(i=503, size=16)
    save_task: asyncio.Task | None = None
    delete_task: asyncio.Task | None = None

    try:
        save_task = asyncio.create_task(s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx)))
        await commit_gate.wait_until_entered()

        delete_task = asyncio.create_task(s.delete_context(ctx.workflow_id))
        await asyncio.sleep(0)
        assert not delete_task.done()

        commit_gate.release()
        await asyncio.wait_for(asyncio.gather(save_task, delete_task), timeout=2.0)

        rows = await s.get_all_contexts()
        assert all(row.workflow_id != ctx.workflow_id for row in rows)
        assert commit_gate.operation_order == [
            ("upsert", ctx.workflow_id),
            ("delete", ctx.workflow_id),
        ]
    finally:
        await commit_gate.release_and_cleanup_tasks(save_task, delete_task)


async def test_flush_waits_for_worker_after_commit_batch_is_popped_from_queue(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(batch_max_queued_writes=1)

    # prevent timer flush so _flush must wait on the already active worker commit
    s._batch_max_flush_delay_ms = 3_000

    commit_gate = _BlockedCommitGate(s, monkeypatch)
    ctx = mk_ctx(i=504, size=16)
    save_task: asyncio.Task | None = None
    flush_task: asyncio.Task | None = None

    try:
        save_task = asyncio.create_task(s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx)))
        await commit_gate.wait_until_entered()
        assert not s._batch_commit_queue
        assert s._batch_commit_queue_pending_writes == 1

        flush_task = asyncio.create_task(s._flush())
        await asyncio.sleep(0)
        assert not flush_task.done()

        commit_gate.release()
        await asyncio.wait_for(flush_task, timeout=1.0)
        await asyncio.wait_for(save_task, timeout=1.0)

        rows = await s.get_all_contexts()
        assert any(row.workflow_id == ctx.workflow_id for row in rows)
    finally:
        await commit_gate.release_and_cleanup_tasks(save_task, flush_task)


async def test_same_batch_delete_then_save_keeps_row(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    original_ctx = mk_ctx(i=406, size=16)
    # Persist the original row immediately before testing same-batch coalescing.
    s._batch_max_queued_writes = 1
    await s.save_context(original_ctx.workflow_id, original_ctx.minion_composite_key, blob_for(original_ctx))

    # Keep the delete and replacement save in one batch.
    s._batch_max_queued_writes = 2
    s._batch_max_flush_delay_ms = 3_000

    updated_ctx = mk_ctx(i=406, size=32)
    delete_task = asyncio.create_task(s.delete_context(original_ctx.workflow_id))
    await asyncio.sleep(0)
    save_task = asyncio.create_task(
        s.save_context(updated_ctx.workflow_id, updated_ctx.minion_composite_key, blob_for(updated_ctx))
    )

    await asyncio.gather(delete_task, save_task)

    rows = await s.get_all_contexts()
    row = next((row for row in rows if row.workflow_id == updated_ctx.workflow_id), None)
    assert row is not None
    assert deserialize_workflow_context_blob(row.context) == updated_ctx


async def test_same_batch_save_then_delete_removes_row(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger(batch_max_queued_writes=2)

    # keep the save and delete in one batch
    s._batch_max_flush_delay_ms = 3_000

    ctx = mk_ctx(i=407, size=16)
    save_task = asyncio.create_task(
        s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
    )
    await asyncio.sleep(0)
    delete_task = asyncio.create_task(s.delete_context(ctx.workflow_id))

    await asyncio.gather(save_task, delete_task)

    rows = await s.get_all_contexts()
    assert all(row.workflow_id != ctx.workflow_id for row in rows)


async def test_commit_failure_rolls_back_transaction_and_settles_futures(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger()
    good_ctx = mk_ctx(i=403)
    first_completion = asyncio.get_running_loop().create_future()
    failed_completion = asyncio.get_running_loop().create_future()
    first_item = PendingWrite(
        good_ctx.workflow_id,
        "upsert",
        good_ctx.minion_composite_key,
        blob_for(good_ctx),
        first_completion,
    )
    failed_item = PendingWrite("wf-bad", "upsert", "orch-bad", b"bad", failed_completion)
    assert s._db is not None
    # Patch the private connection to fail only after the first batch write succeeds.
    original_execute = s._db.execute
    upsert_count = 0

    async def failing_execute(sql, params=None):
        nonlocal upsert_count
        if sql == SQL_WORKFLOW_UPSERT:
            upsert_count += 1
            if upsert_count == 2:
                raise sqlite3.OperationalError("upsert boom")
        if params is None:
            return await original_execute(sql)
        return await original_execute(sql, params)

    monkeypatch.setattr(s._db, "execute", failing_execute)

    with pytest.raises(sqlite3.OperationalError, match="upsert boom"):
        await s._commit_batch_and_settle([first_item, failed_item])

    assert first_completion.done()
    assert failed_completion.done()
    with pytest.raises(sqlite3.OperationalError, match="upsert boom"):
        await first_completion
    with pytest.raises(sqlite3.OperationalError, match="upsert boom"):
        await failed_completion

    monkeypatch.setattr(s._db, "execute", original_execute)

    rows = await s.get_all_contexts()
    assert all(row.workflow_id != good_ctx.workflow_id for row in rows)

    ctx = mk_ctx(i=404)
    await s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
    rows = await s.get_all_contexts()
    assert any(row.workflow_id == ctx.workflow_id for row in rows)


async def test_shutdown_waits_for_active_scheduled_flush_commit(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        batch_max_flush_delay_ms=5,
    )

    commit_gate = _BlockedCommitGate(s, monkeypatch)
    ctx = mk_ctx(i=405)
    save_task: asyncio.Task | None = None
    shutdown_task: asyncio.Task | None = None

    try:
        save_task = asyncio.create_task(
            s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
        )
        await commit_gate.wait_until_entered()

        shutdown_task = asyncio.create_task(s._mn_shutdown())
        await asyncio.sleep(0)

        assert not shutdown_task.done()
        commit_gate.release()
        await asyncio.wait_for(shutdown_task, timeout=1.0)
        await asyncio.wait_for(save_task, timeout=1.0)
    finally:
        await commit_gate.release_and_cleanup_tasks(shutdown_task, save_task)


async def test_shutdown_flushes_pending_batch_buffer(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    # Keep the save buffered until shutdown performs the final flush.
    s._batch_max_flush_delay_ms = 10_000
    ctx = mk_ctx(i=42)
    save_task = asyncio.create_task(
        s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
    )
    await asyncio.sleep(0)
    assert s._batch_buffer
    assert not save_task.done()
    await s._mn_shutdown()
    await save_task

    logger2 = InMemoryLogger()
    s2 = SQLiteStateStore(db_path=s.db_path, logger=logger2)
    await logger2._mn_startup()
    await s2._mn_startup()
    rows = await s2.get_all_contexts()
    await s2._mn_shutdown()
    await logger2._mn_shutdown()
    assert any(row.workflow_id == "wf-42" for row in rows)
