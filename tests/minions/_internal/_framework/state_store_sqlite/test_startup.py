import os
import sqlite3

import pytest

from minions._internal._framework.state_store_sqlite import (
    CommitMeasurementProbeCleanupError,
    DEFAULT_BATCH_MAX_FLUSH_DELAY_MS,
    DEFAULT_BATCH_MAX_QUEUED_WRITES,
    SQL_WORKFLOW_DELETE,
    SQL_WORKFLOW_UPSERT,
    SQL_WORKFLOWS_TABLE_CREATE_IF_NOT_EXISTS,
    SQLiteStateStore,
    WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
    WORKFLOW_ID_STARTUP_PROBE,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.minions._internal._framework.state_store_sqlite._support import StartupProbeDb
from tests.minions._internal._framework.state_store_sqlite.conftest import MakeStateStoreAndLogger


pytestmark = pytest.mark.asyncio


# Startup Contract / Schema


async def test_startup_creates_table_index_and_collects_measurements_even_in_manual_mode(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, logger = await make_state_store_and_logger()
    assert s._db is not None
    assert s._batch_max_queued_writes is not None and s._batch_max_flush_delay_ms is not None
    assert s._batch_max_queued_writes == DEFAULT_BATCH_MAX_QUEUED_WRITES
    assert s._batch_max_flush_delay_ms == DEFAULT_BATCH_MAX_FLUSH_DELAY_MS
    assert s._sqlite_page_size is not None and s._sqlite_page_size >= 1024
    assert any(
        "p50_commit_ms" in log.kwargs
        and log.kwargs.get("measurement_result") == "measured"
        and log.kwargs.get("batch_tuning") == "manual"
        for log in logger.logs
    )

    async with s._db.execute("PRAGMA index_list(workflows)") as c:
        rows = await c.fetchall()
    assert any(row[1] == "idx_workflows_orchestration_id" for row in rows)


async def test_startup_scrubs_orphaned_probe_rows(tmp_path):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    conn = sqlite3.connect(db_path)
    try:
        conn.execute(SQL_WORKFLOWS_TABLE_CREATE_IF_NOT_EXISTS)
        conn.execute(
            SQL_WORKFLOW_UPSERT,
            (WORKFLOW_ID_STARTUP_PROBE, WORKFLOW_ID_STARTUP_PROBE, b"orphaned-startup-probe"),
        )
        conn.execute(
            SQL_WORKFLOW_UPSERT,
            (
                WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
                WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
                b"orphaned-commit-measurement-probe",
            ),
        )
        conn.execute(SQL_WORKFLOW_UPSERT, ("wf-real", "orch-real", b"real-context"))
        conn.commit()
    finally:
        conn.close()

    s = SQLiteStateStore(db_path=db_path, logger=logger)
    await logger._mn_startup()
    await s._mn_startup()
    try:
        rows = await s.get_all_contexts()
        assert {(row.workflow_id, row.orchestration_id, row.context) for row in rows} == {
            ("wf-real", "orch-real", b"real-context"),
        }
    finally:
        await s._mn_shutdown()
        await logger._mn_shutdown()


# SQLite PRAGMA Configuration


async def test_startup_applies_configured_wal_autocheckpoint(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    configured_pages = 2048
    s, _ = await make_state_store_and_logger(
        sqlite_wal_autocheckpoint=configured_pages,
    )
    assert s._db is not None
    async with s._db.execute("PRAGMA wal_autocheckpoint") as c:
        row = await c.fetchone()
    assert row is not None
    assert int(row[0]) == configured_pages


async def test_startup_applies_configured_busy_timeout(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    configured_ms = 7500
    s, _ = await make_state_store_and_logger(
        sqlite_busy_timeout_ms=configured_ms,
    )
    assert s._db is not None
    async with s._db.execute("PRAGMA busy_timeout") as c:
        row = await c.fetchone()
    assert row is not None
    assert int(row[0]) == configured_ms


async def test_startup_applies_configured_journal_mode(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    configured_mode = "DELETE"
    s, _ = await make_state_store_and_logger(
        sqlite_journal_mode=configured_mode,
    )
    assert s._db is not None
    async with s._db.execute("PRAGMA journal_mode") as c:
        row = await c.fetchone()
    assert row is not None
    assert str(row[0]).upper() == configured_mode


async def test_startup_applies_configured_synchronous(
    make_state_store_and_logger: MakeStateStoreAndLogger,
):
    s, _ = await make_state_store_and_logger(
        sqlite_synchronous="OFF",
    )
    assert s._db is not None
    async with s._db.execute("PRAGMA synchronous") as c:
        row = await c.fetchone()
    assert row is not None
    assert int(row[0]) == 0


# Measurement Fallback Policy


async def test_startup_degrades_when_page_size_lookup_fails(tmp_path, monkeypatch):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        batch_tuning="calibrated",
    )

    async def _boom(self):
        raise RuntimeError("page_size boom")

    # Force the private page-size probe to fail so startup must use fallback measurements.
    monkeypatch.setattr(SQLiteStateStore, "_read_page_size", _boom)

    await logger._mn_startup()
    await s._mn_startup()
    try:
        assert s._db is not None
        assert s._sqlite_page_size is None
        assert await logger.wait_for_log("could not read PRAGMA page_size")
        assert any(
            log.kwargs.get("measurement_result") == "fallback_used"
            and log.kwargs.get("sqlite_page_size") is None
            for log in logger.logs
        )
    finally:
        await s._mn_shutdown()
        await logger._mn_shutdown()


async def test_startup_degrades_when_page_size_is_unusable(tmp_path, monkeypatch):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        batch_tuning="calibrated",
    )

    async def _bad_page_size(self):
        return 0

    # Return an unusable private page-size probe result so fallback policy is exercised.
    monkeypatch.setattr(SQLiteStateStore, "_read_page_size", _bad_page_size)

    await logger._mn_startup()
    await s._mn_startup()
    try:
        assert s._sqlite_page_size is None
        assert await logger.wait_for_log("returned unusable value")
        assert any(log.kwargs.get("measurement_result") == "fallback_used" for log in logger.logs)
    finally:
        await s._mn_shutdown()
        await logger._mn_shutdown()


async def test_startup_uses_fallback_measurements_when_commit_timing_fails_without_cleanup_failure(
    tmp_path,
    monkeypatch,
):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        batch_tuning="calibrated",
    )

    async def _boom(self, _page_size):
        raise RuntimeError("timing boom")

    # Force private commit measurement to fail after page-size probing succeeds.
    monkeypatch.setattr(SQLiteStateStore, "_measure_commit_latency_percentiles", _boom)

    await logger._mn_startup()
    await s._mn_startup()
    try:
        assert s._sqlite_page_size is not None and s._sqlite_page_size >= 1024
        assert s._sqlite_page_size is not None
        assert await logger.wait_for_log("startup measurements failed; using fallback values")
        assert any(log.kwargs.get("measurement_result") == "fallback_used" for log in logger.logs)
    finally:
        await s._mn_shutdown()
        await logger._mn_shutdown()


# Probe Cleanup Failure Policy


async def test_measure_commit_latency_percentiles_raises_when_probe_cleanup_delete_fails_after_measurement(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, _ = await make_state_store_and_logger()
    assert s._db is not None
    # Fail only the private cleanup delete after measurement work completes.
    original_execute = s._db.execute

    async def failing_execute(sql, params=None):
        if sql == SQL_WORKFLOW_DELETE and params == (WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,):
            raise RuntimeError("delete boom")
        if params is None:
            return await original_execute(sql)
        return await original_execute(sql, params)

    monkeypatch.setattr(s._db, "execute", failing_execute)

    with pytest.raises(CommitMeasurementProbeCleanupError, match="commit measurement probe cleanup failed"):
        await s._measure_commit_latency_percentiles(4096)


async def test_measure_commit_latency_percentiles_raises_when_measurement_and_probe_cleanup_fail(
    make_state_store_and_logger: MakeStateStoreAndLogger,
    monkeypatch,
):
    s, logger = await make_state_store_and_logger()
    assert s._db is not None
    # Fail both the private measurement transaction and its cleanup delete.
    original_execute = s._db.execute

    async def failing_execute(sql, params=None):
        if sql == SQL_WORKFLOW_DELETE and params == (WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,):
            raise RuntimeError("delete boom")
        if sql == "BEGIN IMMEDIATE":
            raise RuntimeError("measure boom")
        if params is None:
            return await original_execute(sql)
        return await original_execute(sql, params)

    monkeypatch.setattr(s._db, "execute", failing_execute)

    with pytest.raises(CommitMeasurementProbeCleanupError, match="commit measurement probe cleanup failed") as exc:
        await s._measure_commit_latency_percentiles(4096)

    assert isinstance(exc.value.__cause__, RuntimeError)
    assert str(exc.value.__cause__) == "delete boom"
    assert any(
        "commit measurement probe cleanup failed after probe error" in log.msg
        and log.kwargs.get("measurement_probe_error_type") == "RuntimeError"
        and log.kwargs.get("measurement_probe_error_message") == "measure boom"
        and log.kwargs.get("error_message") == "delete boom"
        for log in logger.logs
    )


# Startup Resource Cleanup


async def test_startup_closes_connection_when_startup_phase_fails(tmp_path, monkeypatch):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(db_path=db_path, logger=logger)
    close_called = False

    async def _boom(self):
        nonlocal close_called
        assert self._db is not None
        # Wrap the private connection close to prove startup cleans up after probe failure.
        original_close = self._db.close

        async def close_wrapper():
            nonlocal close_called
            close_called = True
            await original_close()

        monkeypatch.setattr(self._db, "close", close_wrapper)
        raise RuntimeError("startup boom")

    # Fail a later private startup phase after the connection has been opened.
    monkeypatch.setattr(SQLiteStateStore, "_run_startup_probe", _boom)

    with pytest.raises(RuntimeError, match="startup boom"):
        await s.startup()

    assert close_called
    assert s._db is None


async def test_startup_propagates_commit_measurement_probe_cleanup_failure(tmp_path, monkeypatch):
    logger = InMemoryLogger()
    db_path = os.path.join(tmp_path, "state.db")
    s = SQLiteStateStore(
        db_path=db_path,
        logger=logger,
        batch_tuning="calibrated",
    )

    async def _cleanup_boom(self, _page_size):
        raise CommitMeasurementProbeCleanupError("cleanup boom")

    # Make the private measurement helper raise a non-fallback cleanup failure.
    monkeypatch.setattr(SQLiteStateStore, "_measure_commit_latency_percentiles", _cleanup_boom)

    try:
        with pytest.raises(CommitMeasurementProbeCleanupError, match="cleanup boom"):
            await s.startup()
        assert not any("startup measurements failed; using fallback values" in log.msg for log in logger.logs)
    finally:
        if s._db is not None:
            # Startup failed before normal shutdown ownership, so close the private connection manually.
            await s._db.close()
            s._db = None


async def test_startup_probe_raises_when_cleanup_delete_fails(monkeypatch):
    logger = InMemoryLogger()
    s = SQLiteStateStore(db_path=":memory:", logger=logger)
    fake_db = StartupProbeDb(
        select_row=(WORKFLOW_ID_STARTUP_PROBE, b"startup-probe"),
        delete_error=RuntimeError("delete boom"),
    )
    # Inject a fake private DB adapter to isolate startup probe cleanup handling.
    monkeypatch.setattr(s, "_require_db", lambda: fake_db)

    with pytest.raises(RuntimeError, match="startup probe cleanup failed"):
        await s._run_startup_probe()


async def test_startup_probe_preserves_primary_failure_when_cleanup_delete_also_fails(monkeypatch):
    logger = InMemoryLogger()
    s = SQLiteStateStore(db_path=":memory:", logger=logger)
    fake_db = StartupProbeDb(
        select_row=("wrong-id", b"wrong-payload"),
        delete_error=RuntimeError("delete boom"),
    )
    # Inject a fake private DB adapter to force both probe mismatch and cleanup failure.
    monkeypatch.setattr(s, "_require_db", lambda: fake_db)

    with pytest.raises(RuntimeError, match="startup probe failed to round-trip persisted workflow state"):
        await s._run_startup_probe()

    assert any(
        "startup probe cleanup failed after probe error" in log.msg
        and log.kwargs.get("startup_probe_error_type") == "RuntimeError"
        and log.kwargs.get("error_type") == "RuntimeError"
        for log in logger.logs
    )
