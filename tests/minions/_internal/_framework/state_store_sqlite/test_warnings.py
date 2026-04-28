import asyncio

import pytest

from minions._internal._framework.logger import CRITICAL, WARNING
from minions._internal._framework.state_store_sqlite import SQLiteStateStore, WarnThreshold
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.minions._internal._framework.state_store_sqlite._support import blob_for, mk_ctx
from tests.minions._internal._framework.state_store_sqlite.conftest import MakeStateStoreAndLogger


pytestmark = pytest.mark.asyncio


async def test_large_state_warning(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, logger = await make_state_store_and_logger(warn_size_pages=1)
    # Force a deterministic page-count warning without depending on the host SQLite page size.
    s._sqlite_page_size = 4096
    big = mk_ctx(i=10, size=8192)
    await s.save_context(big.workflow_id, big.minion_composite_key, blob_for(big))
    assert await logger.wait_for_log("Large workflow context blob detected"), "expected large-state warning"


async def test_large_state_warning_is_disabled_without_page_size(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, logger = await make_state_store_and_logger(warn_size_pages=1)
    # Simulate startup being unable to determine page size; size warnings must stay disabled.
    s._sqlite_page_size = None
    big = mk_ctx(i=11, size=8192)
    await s.save_context(big.workflow_id, big.minion_composite_key, blob_for(big))
    assert not await logger.wait_for_log("Large workflow context blob detected", timeout=0.05)


async def test_save_context_records_commit_latency_metric(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, _ = await make_state_store_and_logger()
    for i in range(5):
        ctx = mk_ctx(i=i)
        await s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))

    assert s._metric_commit_latency_ms_hist, "expected commit timings recorded"


async def _queue_three_writes(s: SQLiteStateStore) -> list[asyncio.Task]:
    tasks = []
    for i in range(1, 4):
        ctx = mk_ctx(i=i)
        tasks.append(
            asyncio.create_task(
                s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
            )
        )
    return tasks


async def test_queued_writes_warning_counts_batch_buffer(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, logger = await make_state_store_and_logger(batch_max_queued_writes=100)

    # Lower the queue warning threshold so three buffered writes trigger the signal.
    s._warn_write_thresholds.queued_writes = WarnThreshold(warn=2, crit=999)
    # Keep writes queued so the warning observes the batch buffer before a timer flush.
    s._batch_max_flush_delay_ms = 10_000

    tasks = await _queue_three_writes(s)
    try:
        assert await logger.wait_for_log("queued_writes="), "expected queued-writes warning"
        assert any(
            "queued_writes=3>warn(2)" in log.msg and log.level == WARNING
            for log in logger.logs
        )
    finally:
        await s._flush()
        await asyncio.gather(*tasks)


async def test_queued_writes_critical_counts_batch_buffer(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, logger = await make_state_store_and_logger(batch_max_queued_writes=100)

    # Make the first emitted queued-write pressure log critical at three buffered writes.
    s._warn_write_thresholds.queued_writes = WarnThreshold(warn=2, crit=2)
    # Keep writes queued so the warning observes the batch buffer before a timer flush.
    s._batch_max_flush_delay_ms = 10_000

    tasks = await _queue_three_writes(s)
    try:
        assert await logger.wait_for_log("queued_writes="), "expected queued-writes critical warning"
        assert any(
            "queued_writes=3>warn(2)" in log.msg and log.level == CRITICAL
            for log in logger.logs
        )
    finally:
        await s._flush()
        await asyncio.gather(*tasks)


async def test_queued_writes_critical_bypasses_warning_cooldown(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, logger = await make_state_store_and_logger(
        batch_max_queued_writes=100,
        warn_write_cooldown_s=60.0,
    )

    # Let the second queued write warn, then require the third write to escalate.
    s._warn_write_thresholds.queued_writes = WarnThreshold(warn=1, crit=2)
    # Keep writes queued so the warning observes the batch buffer before a timer flush.
    s._batch_max_flush_delay_ms = 10_000

    tasks = await _queue_three_writes(s)
    try:
        assert await logger.wait_for_log("queued_writes=2>warn(1)"), "expected queued-writes warning"
        assert any(
            "queued_writes=2>warn(1)" in log.msg and log.level == WARNING
            for log in logger.logs
        )

        assert await logger.wait_for_log("queued_writes=3>warn(1)"), "expected queued-writes critical"
        assert any(
            "queued_writes=3>warn(1)" in log.msg and log.level == CRITICAL
            for log in logger.logs
        )
    finally:
        await s._flush()
        await asyncio.gather(*tasks)


async def test_queued_writes_warning_counts_batches_waiting_for_commit(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, logger = await make_state_store_and_logger(
        batch_max_queued_writes=2,
        batch_max_flush_delay_ms=40,
    )

    commit_entered = asyncio.Event()
    allow_commit = asyncio.Event()
    # Block the commit worker so queued-write pressure includes batches waiting to commit.
    original_commit_batch_now = s._commit_batch_now

    async def wrapped_commit_batch_now(items):
        commit_entered.set()
        await allow_commit.wait()
        return await original_commit_batch_now(items)

    monkeypatch.setattr(s, "_commit_batch_now", wrapped_commit_batch_now)
    tasks = [
        asyncio.create_task(
            s.save_context(ctx.workflow_id, ctx.minion_composite_key, blob_for(ctx))
        )
        for ctx in (mk_ctx(i=i) for i in range(10))
    ]

    try:
        await asyncio.wait_for(commit_entered.wait(), timeout=1.0)
        assert await logger.wait_for_log("queued_writes=", timeout=1.0)
        assert any("queued_writes=" in log.msg and ">warn(8)" in log.msg for log in logger.logs)
        allow_commit.set()
        await asyncio.wait_for(asyncio.gather(*tasks), timeout=2.0)
    finally:
        allow_commit.set()
        await asyncio.gather(*tasks, return_exceptions=True)


async def test_commit_p95_warning(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, logger = await make_state_store_and_logger()
    # Isolate the p95 signal by disabling the rows/sec warning path.
    s._warn_write_thresholds.commit_p95_ms = WarnThreshold(warn=5.0, crit=9999.0)
    s._warn_write_thresholds.rows_per_sec = WarnThreshold(
        warn=float("inf"),
        crit=float("inf"),
    )

    # Seed enough samples for the percentile calculation to emit a warning deterministically.
    s._metric_commit_latency_ms_hist.extend([10.0] * 22)

    s._check_perf_signals()
    assert await logger.wait_for_log("commit_p95=", timeout=1.0), "expected commit_p95 warning to fire"


async def test_rows_per_sec_warning(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, logger = await make_state_store_and_logger()
    # Isolate the rows/sec signal by keeping the commit-p95 path below its sample threshold.
    s._warn_write_thresholds.rows_per_sec = WarnThreshold(warn=5, crit=9999)
    s._metric_rows_per_sec_hist.extend([10, 10, 10])

    s._check_perf_signals()

    assert await logger.wait_for_log("rows_per_sec=", timeout=1.0), "expected rows/sec warning to fire"
    assert any(
        "rows_per_sec=10>warn(5)" in log.msg and log.level == WARNING
        for log in logger.logs
    )


async def test_write_pressure_logs_warning_and_escalates_critical_during_cooldown(make_state_store_and_logger: MakeStateStoreAndLogger):
    s, logger = await make_state_store_and_logger(warn_write_cooldown_s=60.0)

    s._maybe_warn_write_pressure("warn-path")
    assert await logger.wait_for_log("warn-path", timeout=1.0)
    assert any("warn-path" in log.msg and log.level == WARNING for log in logger.logs)

    # Critical pressure should bypass cooldown set by a warning-level event.
    s._maybe_warn_write_pressure("critical-path", critical=True)
    assert await logger.wait_for_log("critical-path", timeout=1.0)
    assert any("critical-path" in log.msg and log.level == CRITICAL for log in logger.logs)


async def test_write_pressure_cooldown_suppresses_repeated_critical_and_lower_severity(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, logger = await make_state_store_and_logger(warn_write_cooldown_s=60.0)

    s._maybe_warn_write_pressure("critical-path", critical=True)
    assert await logger.wait_for_log("critical-path", timeout=1.0)

    s._maybe_warn_write_pressure("critical-repeat", critical=True)
    s._maybe_warn_write_pressure("warn-after-critical")

    assert not await logger.wait_for_log("critical-repeat", timeout=0.05)
    assert not await logger.wait_for_log("warn-after-critical", timeout=0.05)
