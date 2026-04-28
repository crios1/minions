import aiosqlite
import asyncio
import statistics
import time

from collections.abc import Iterable
from collections import deque
from dataclasses import dataclass, field, replace
from typing import Literal, cast

from .logger import CRITICAL, DEBUG, ERROR, WARNING, Logger
from .state_store import StateStore, StoredWorkflowContext
from .._utils.format_exception_traceback import format_exception_traceback
from .._utils.safe_create_task import safe_create_task

SQL_WORKFLOWS_TABLE_CREATE_IF_NOT_EXISTS = """
    CREATE TABLE IF NOT EXISTS workflows(
        workflow_id TEXT PRIMARY KEY,
        orchestration_id TEXT NOT NULL,
        context BLOB NOT NULL
    )
"""
SQL_ORCHESTRATION_INDEX_CREATE_IF_NOT_EXISTS = """
    CREATE INDEX IF NOT EXISTS idx_workflows_orchestration_id
    ON workflows(orchestration_id)
"""
SQL_WORKFLOW_UPSERT = """
    INSERT INTO workflows(workflow_id, orchestration_id, context)
    VALUES(?, ?, ?)
    ON CONFLICT(workflow_id) DO UPDATE SET
        orchestration_id=excluded.orchestration_id,
        context=excluded.context
"""
SQL_WORKFLOW_INSERT_IF_NOT_EXISTS = """
    INSERT INTO workflows(workflow_id, orchestration_id, context)
    VALUES(?, ?, ?)
    ON CONFLICT(workflow_id) DO NOTHING
"""
SQL_WORKFLOWS_SELECT_FOR_ORCHESTRATION = """
    SELECT workflow_id, orchestration_id, context
    FROM workflows
    WHERE orchestration_id = ?
"""
SQL_WORKFLOW_SELECT_BY_ID = """
    SELECT orchestration_id, context
    FROM workflows
    WHERE workflow_id = ?
"""
SQL_WORKFLOWS_SELECT_ALL = "SELECT workflow_id, orchestration_id, context FROM workflows"
SQL_WORKFLOW_DELETE = "DELETE FROM workflows WHERE workflow_id = ?"

WORKFLOW_ID_STARTUP_PROBE = "__state_store_startup_probe__"
WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE = "__state_store_commit_measurement_probe__"
WORKFLOW_IDS_PROBE = (WORKFLOW_ID_STARTUP_PROBE, WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE)

BATCH_MAX_QUEUED_WRITES_FLOOR = 1
BATCH_MAX_QUEUED_WRITES_RECOMMENDED_FLOOR = 16
BATCH_MAX_QUEUED_WRITES_CEILING = 256
BATCH_MAX_FLUSH_DELAY_MS_FLOOR = 5
BATCH_MAX_FLUSH_DELAY_MS_CEILING = 40

DEFAULT_BATCH_MAX_QUEUED_WRITES = 64
DEFAULT_BATCH_MAX_FLUSH_DELAY_MS = 8

BatchTuningMode = Literal["manual", "calibrated"]
_ALLOWED_BATCH_TUNING_MODES: tuple[BatchTuningMode, ...] = (
    "manual",
    "calibrated",
)

BATCH_MAX_QUEUED_WRITES_NVME_LIKE = 48
BATCH_MAX_FLUSH_DELAY_MS_NVME_LIKE = 5
BATCH_MAX_QUEUED_WRITES_SSD_LIKE = 64
BATCH_MAX_FLUSH_DELAY_MS_SSD_LIKE = 8
BATCH_MAX_QUEUED_WRITES_HDD_LIKE = 128
BATCH_MAX_FLUSH_DELAY_MS_HDD_LIKE = 16

COMMIT_P50_MS_MAX_NVME_LIKE = 2.0
COMMIT_P50_MS_MAX_SSD_LIKE = 6.0


@dataclass(frozen=True)
class PendingWrite:
    workflow_id: str
    op: Literal["upsert", "delete"]
    orchestration_id: str | None
    payload: bytes | None
    completion: asyncio.Future[None]


@dataclass(frozen=True)
class StartupMeasurements:
    commit_p50_ms: float
    commit_p95_ms: float
    page_size: int | None
    result: Literal["measured", "fallback_used"]


@dataclass
class WarnThreshold:
    warn: float = float("inf")
    crit: float = float("inf")


@dataclass
class WriteWarnThresholds:
    commit_p95_ms: WarnThreshold = field(default_factory=WarnThreshold)
    queued_writes: WarnThreshold = field(default_factory=WarnThreshold)
    rows_per_sec: WarnThreshold = field(default_factory=WarnThreshold)


class CommitMeasurementProbeCleanupError(RuntimeError):
    pass


class SQLiteStateStore(StateStore):
    """
    SQLite-backed state store for workflow contexts.

    Design Goals
    ------------
    - **Single long-lived connection** tuned with WAL + synchronous=NORMAL
    - **BLOB storage** (binary-encoded payloads via `msgspec`)
    - **Micro-batching**:
        * Buffer up to `batch_max_queued_writes` contexts or `batch_max_flush_delay_ms` elapsed time
        * Commit each flushed batch atomically; if one write fails, the whole batch rolls back.
        * `batch_max_queued_writes=1` disables coalescing and uses one-row transactions.
    - **Startup measurements**:
        * Measure median/p95 commit latency
        * In `batch_tuning="calibrated"`, pick sensible batch size/window based on disk speed
          (NVMe-like -> smaller batches with shorter flush windows,
          SSD-like -> medium batches with moderate flush windows,
          HDD-like -> larger batches with longer flush windows)
    - **Hardware-relative warnings**:
        * Detect when system is under more load than SQLiteStateStore can sustain
        * Thresholds scale with startup measurements so they work across NVMe/SSD/HDD

    Why batching?
    -------------
    - Without batching: every state update -> its own transaction/fsync
      = high overhead, quickly I/O-bound.
    - With batching: amortize commit cost over many updates
      = 3-10x throughput increase depending on hardware.

    Warning Signals
    ---------------
    Warnings are not "your code is broken," they're "SQLiteStateStore can't keep up anymore."
    They help decide when to tune caps or use a stronger store.

    - **Commit p95 (ms)**:
        Fires if live p95 commit time > ~3x startup-measured baseline.
        Indicates storage slowness or single-writer contention.
    - **Queued Writes (rows)**:
        Fires if in-memory buffer grows beyond ~4x batch_max_queued_writes.
        Indicates producers are faster than SQLiteStateStore can _flush.
        Only visible with batching.
    - **Rows/sec capacity**:
        Fires if sustained write rate exceeds startup-measured device capacity.
        Indicates you're outgrowing SQLiteStateStore's throughput ceiling.

    Operator Actions
    ----------------
    1. If latency budget allows -> increase batch caps (`batch_max_queued_writes`, `batch_max_flush_delay_ms`)
    2. Confirm WAL + synchronous=NORMAL, consider adjusting wal_autocheckpoint

    Summary
    -------
    - Startup measurements power hardware-relative warnings and can tune batching in calibrated mode
    - Micro-batching improves throughput
    - Warnings give clear "you're outgrowing SQLiteStateStore" signals, scaled to hardware
    """

    def __init__(
        self,
        db_path: str,
        logger: Logger,
        *,
        batch_tuning: BatchTuningMode = "manual",
        batch_max_queued_writes: int | None = None,
        batch_max_flush_delay_ms: int | None = None,
        batch_max_interarrival_delay_ms: int | None = None,
        sqlite_journal_mode: str = "WAL",
        sqlite_synchronous: str = "NORMAL",
        sqlite_wal_autocheckpoint: int = 1000,
        sqlite_busy_timeout_ms: int = 3000,
        warn_size_pages: int = 32,
        warn_size_crit_pages: int = 256,
        warn_size_cooldown_s: float = 3600.0,
        warn_write_cooldown_s: float = 30.0,
    ):
        """
        Create a SQLite-backed state store.

        Batch tuning defaults to manual, SSD-like settings. In manual mode,
        omitted batch settings (`None`) resolve to
        `DEFAULT_BATCH_MAX_QUEUED_WRITES` and `DEFAULT_BATCH_MAX_FLUSH_DELAY_MS`.
        Set `batch_max_queued_writes=1` for one-row transactions with no
        batching delay. Values from 2 through 15 are allowed for tight latency
        budgets but are below the recommended batching range and will log a
        startup warning.
        In calibrated mode, batch settings must be omitted and are resolved from
        startup commit-latency measurements.
        """
        super().__init__(logger)
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

        self._batch_tuning = self._resolve_batch_tuning_mode(batch_tuning)
        self._batch_max_queued_writes_was_configured = batch_max_queued_writes is not None
        (
            self._batch_max_queued_writes,
            self._batch_max_flush_delay_ms,
        ) = self._resolve_batch_config_from_config(
            batch_max_queued_writes,
            batch_max_flush_delay_ms,
        )
        self._batch_max_interarrival_delay_ms = batch_max_interarrival_delay_ms

        self._batch_buffer: list[PendingWrite] = []
        self._batch_buffer_lock = asyncio.Lock()
        self._batch_buffer_flush_task: asyncio.Task[None] | None = None  # scheduled flush task for the batch buffer
        self._batch_buffer_flush_task_is_flushing = False
        self._batch_buffer_flush_deadline: float | None = None
        self._batch_buffer_last_enqueue_at: float | None = None

        self._batch_commit_queue: deque[list[PendingWrite]] = deque()
        self._batch_commit_queue_worker_task: asyncio.Task[None] | None = None
        self._batch_commit_queue_pending_writes = 0

        self._batch_commit_execution_lock = asyncio.Lock()

        self._metric_commit_latency_ms_hist: deque[float] = deque(maxlen=200)
        self._metric_rows_per_sec_hist: deque[int] = deque(maxlen=60)
        self._metric_rows_per_sec_window_ts = int(time.monotonic())
        self._metric_rows_per_sec_window_rows = 0

        self._warn_write_thresholds = WriteWarnThresholds()
        self._warn_write_cooldown_s = warn_write_cooldown_s
        self._warn_write_last_at = 0.0
        self._warn_write_last_critical = False

        self._warn_size_pages = warn_size_pages
        self._warn_size_crit_pages = warn_size_crit_pages
        self._warn_size_last_at_by_workflow_id: dict[str, float] = {}
        self._warn_size_cooldown_s = warn_size_cooldown_s

        self._sqlite_page_size: int | None = None
        self._sqlite_wal_autocheckpoint = sqlite_wal_autocheckpoint
        self._sqlite_busy_timeout_ms = sqlite_busy_timeout_ms
        self._sqlite_journal_mode = sqlite_journal_mode
        self._sqlite_synchronous = sqlite_synchronous

    # StateStore Contract

    async def startup(self) -> None:
        self._db = await aiosqlite.connect(self.db_path)
        try:
            await self._apply_pragmas()
            await self._ensure_schema()
            await self._scrub_orphaned_probe_rows()
            await self._run_startup_probe()

            measurements = await self._collect_startup_measurements()
            self._sqlite_page_size = measurements.page_size
            self._resolve_batch_config_from_measurements(measurements)
            self._apply_write_warn_thresholds(measurements)
            await self._log_startup_measurements_summary(measurements)
            await self._log_low_batch_max_queued_writes_warning()
        except BaseException as startup_error:
            db = self._db
            self._db = None
            if db is not None:
                try:
                    await db.close()
                except Exception as close_error:
                    await self._mn_logger._log(
                        ERROR,
                        f"{type(self).__name__} failed to close SQLite connection after startup failure",
                        error_type=type(close_error).__name__,
                        error_message=str(close_error),
                        traceback=format_exception_traceback(close_error),
                        startup_error_type=type(startup_error).__name__,
                        startup_error_message=str(startup_error),
                    )
            raise

    async def shutdown(self) -> None:
        await self._flush()
        if (
            self._batch_buffer_flush_task
            and not self._batch_buffer_flush_task.done()
            and not self._batch_buffer_flush_task_is_flushing
        ):
            self._batch_buffer_flush_task.cancel()
            await asyncio.gather(self._batch_buffer_flush_task, return_exceptions=True) # suppress expected CancelledError from the cancelled background flush task
        if self._batch_buffer_flush_task and not self._batch_buffer_flush_task.done():
            await asyncio.gather(self._batch_buffer_flush_task, return_exceptions=True)
        self._batch_buffer_flush_task = None
        await self._await_batch_commit_queue_drained()
        if self._db is not None:
            await self._db.close()
            self._db = None

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ) -> None:
        self._maybe_warn_large_payload(workflow_id, orchestration_id, context)
        fut = await self._enqueue_batch_entry(
            workflow_id,
            "upsert",
            orchestration_id,
            context,
        )
        await fut

    async def delete_context(self, workflow_id: str) -> None:
        fut = await self._enqueue_batch_entry(workflow_id, "delete", None, None)
        await fut

    async def get_contexts_for_orchestration(
        self,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        await self._flush()
        db = self._require_db()
        async with db.execute(
            SQL_WORKFLOWS_SELECT_FOR_ORCHESTRATION,
            (orchestration_id,),
        ) as cursor:
            rows = await cursor.fetchall()
        return self._rows_to_stored_contexts(rows)

    async def get_all_contexts(self) -> list[StoredWorkflowContext]:
        await self._flush()
        db = self._require_db()
        async with db.execute(SQL_WORKFLOWS_SELECT_ALL) as cursor:
            rows = await cursor.fetchall()
        return self._rows_to_stored_contexts(rows)

    def _rows_to_stored_contexts(
        self,
        rows: Iterable[aiosqlite.Row],
    ) -> list[StoredWorkflowContext]:
        return [
            StoredWorkflowContext(
                workflow_id=workflow_id,
                orchestration_id=orchestration_id,
                context=bytes(context_blob),
            )
            for workflow_id, orchestration_id, context_blob in rows
        ]

    # Lifecycle and Invariants

    def _require_db(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError(f"{type(self).__name__} db never acquired")
        return self._db

    # SQLite Bootstrap

    async def _apply_pragmas(self) -> None:
        db = self._require_db()
        await db.execute(f"PRAGMA journal_mode={self._sqlite_journal_mode}")
        await db.execute(f"PRAGMA synchronous={self._sqlite_synchronous}")
        await db.execute(f"PRAGMA wal_autocheckpoint={self._sqlite_wal_autocheckpoint}")
        await db.execute(f"PRAGMA busy_timeout={self._sqlite_busy_timeout_ms}")

    async def _ensure_schema(self) -> None:
        db = self._require_db()
        # currently assumes latest schema; add migrations here if schema evolves (e.g. PRAGMA user_version)
        await db.execute(SQL_WORKFLOWS_TABLE_CREATE_IF_NOT_EXISTS)
        await db.execute(SQL_ORCHESTRATION_INDEX_CREATE_IF_NOT_EXISTS)
        await db.commit()

    async def _scrub_orphaned_probe_rows(self) -> None:
        db = self._require_db()
        await db.executemany(
            SQL_WORKFLOW_DELETE,
            [(workflow_id,) for workflow_id in WORKFLOW_IDS_PROBE],
        )
        await db.commit()

    async def _run_startup_probe(self) -> None:
        db = self._require_db()
        payload = b"startup-probe"
        probe_error: Exception | None = None
        try:
            await db.execute(
                SQL_WORKFLOW_UPSERT,
                (WORKFLOW_ID_STARTUP_PROBE, WORKFLOW_ID_STARTUP_PROBE, payload),
            )
            await db.commit()
            async with db.execute(
                SQL_WORKFLOW_SELECT_BY_ID,
                (WORKFLOW_ID_STARTUP_PROBE,),
            ) as cursor:
                row = await cursor.fetchone()
            if row != (WORKFLOW_ID_STARTUP_PROBE, payload):
                raise RuntimeError("startup probe failed to round-trip persisted workflow state")
        except Exception as e:
            probe_error = e
            raise
        finally:
            try:
                await db.execute(SQL_WORKFLOW_DELETE, (WORKFLOW_ID_STARTUP_PROBE,))
                await db.commit()
            except Exception as cleanup_error:
                if probe_error is None:
                    raise RuntimeError(f"{type(self).__name__} startup probe cleanup failed") from cleanup_error
                await self._mn_logger._log(
                    ERROR,
                    f"{type(self).__name__} startup probe cleanup failed after probe error",
                    error_type=type(cleanup_error).__name__,
                    error_message=str(cleanup_error),
                    traceback=format_exception_traceback(cleanup_error),
                    startup_probe_error_type=type(probe_error).__name__,
                    startup_probe_error_message=str(probe_error),
                )

    # Startup Measurements

    async def _collect_startup_measurements(self) -> StartupMeasurements:
        fallback = StartupMeasurements(
            commit_p50_ms=6.0,
            commit_p95_ms=10.0,
            page_size=None,
            result="fallback_used",
        )

        try:
            page_size = await self._read_page_size()
        except Exception as e:
            await self._mn_logger._log(
                WARNING,
                f"{type(self).__name__} startup measurements degraded; could not read PRAGMA page_size",
                traceback=format_exception_traceback(e),
            )
            return fallback

        if page_size is None or page_size <= 0:
            await self._mn_logger._log(
                WARNING,
                f"{type(self).__name__} startup measurements degraded; PRAGMA page_size returned unusable value",
                page_size=page_size,
            )
            return fallback

        try:
            commit_p50_ms, commit_p95_ms = await self._measure_commit_latency_percentiles(page_size)
        except CommitMeasurementProbeCleanupError:
            raise
        except Exception as e:
            await self._mn_logger._log(
                ERROR,
                f"{type(self).__name__} startup measurements failed; using fallback values",
                traceback=format_exception_traceback(e),
            )
            return replace(fallback, page_size=page_size)

        return StartupMeasurements(
            commit_p50_ms=commit_p50_ms,
            commit_p95_ms=commit_p95_ms,
            page_size=page_size,
            result="measured",
        )

    async def _read_page_size(self) -> int | None:
        db = self._require_db()
        async with db.execute("PRAGMA page_size") as cursor:
            row = await cursor.fetchone()
        if row is None:
            return None

        try:
            page_size = int(row[0])
        except (TypeError, ValueError):
            return None

        return page_size if page_size > 0 else None

    async def _measure_commit_latency_percentiles(self, page_size: int) -> tuple[float, float]:
        db = self._require_db()
        await db.executescript("BEGIN IMMEDIATE; ROLLBACK; " * 6)  # warm transaction start/rollback path before timing steady-state commit cost
        measurement_error: Exception | None = None

        async def run_probe(payload: bytes) -> list[float]:
            samples: list[float] = []
            # ensure the measurement row already exists so each probe only measures update costs
            await db.execute(
                SQL_WORKFLOW_INSERT_IF_NOT_EXISTS,
                (
                    WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
                    WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
                    payload,
                ),
            )
            await db.commit()
            for _ in range(8):
                t0 = time.perf_counter()
                await db.execute("BEGIN IMMEDIATE")
                await db.execute(
                    SQL_WORKFLOW_UPSERT,
                    (
                        WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
                        WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,
                        payload,
                    ),
                )
                await db.commit()
                samples.append((time.perf_counter() - t0) * 1000.0)
            return samples

        try:
            small_samples = await run_probe(b"x" * 256)
            medium_samples = await run_probe(b"x" * (page_size * 2))
            samples = small_samples + medium_samples
            return (
                statistics.median(samples),
                statistics.quantiles(samples, n=20, method="inclusive")[-1],
            )
        except Exception as e:
            measurement_error = e
            raise
        finally:
            try:
                await db.execute(SQL_WORKFLOW_DELETE, (WORKFLOW_ID_COMMIT_MEASUREMENT_PROBE,))
                await db.commit()
            except Exception as cleanup_error:
                if measurement_error is not None:
                    await self._mn_logger._log(
                        ERROR,
                        f"{type(self).__name__} commit measurement probe cleanup failed after probe error",
                        error_type=type(cleanup_error).__name__,
                        error_message=str(cleanup_error),
                        traceback=format_exception_traceback(cleanup_error),
                        measurement_probe_error_type=type(measurement_error).__name__,
                        measurement_probe_error_message=str(measurement_error),
                    )
                raise CommitMeasurementProbeCleanupError(
                    f"{type(self).__name__} commit measurement probe cleanup failed"
                ) from cleanup_error

    # Batch Tuning

    def _resolve_batch_tuning_mode(
        self,
        batch_tuning: str,
    ) -> BatchTuningMode:
        if batch_tuning not in _ALLOWED_BATCH_TUNING_MODES:
            raise ValueError(f"batch_tuning must be 'manual' or 'calibrated'; got {batch_tuning!r}")
        return cast(BatchTuningMode, batch_tuning)

    def _validate_batch_config(
        self,
        batch_max_queued_writes: int,
        batch_max_flush_delay_ms: int,
    ) -> None:
        if not BATCH_MAX_QUEUED_WRITES_FLOOR <= batch_max_queued_writes <= BATCH_MAX_QUEUED_WRITES_CEILING:
            raise ValueError(
                "batch_max_queued_writes must be between "
                f"{BATCH_MAX_QUEUED_WRITES_FLOOR} and {BATCH_MAX_QUEUED_WRITES_CEILING}; "
                f"got {batch_max_queued_writes}"
            )
        if not BATCH_MAX_FLUSH_DELAY_MS_FLOOR <= batch_max_flush_delay_ms <= BATCH_MAX_FLUSH_DELAY_MS_CEILING:
            raise ValueError(
                "batch_max_flush_delay_ms must be between "
                f"{BATCH_MAX_FLUSH_DELAY_MS_FLOOR} and {BATCH_MAX_FLUSH_DELAY_MS_CEILING}; "
                f"got {batch_max_flush_delay_ms}"
            )

    async def _log_low_batch_max_queued_writes_warning(self) -> None:
        batch_max_queued_writes = self._batch_max_queued_writes
        if (
            self._batch_tuning != "manual"
            or not self._batch_max_queued_writes_was_configured
            or batch_max_queued_writes is None
            or batch_max_queued_writes == BATCH_MAX_QUEUED_WRITES_FLOOR
            or batch_max_queued_writes >= BATCH_MAX_QUEUED_WRITES_RECOMMENDED_FLOOR
        ):
            return

        await self._mn_logger._log(
            WARNING,
            f"{type(self).__name__} batch_max_queued_writes is below the recommended batching range",
            batch_max_queued_writes=batch_max_queued_writes,
            immediate_writes_value=BATCH_MAX_QUEUED_WRITES_FLOOR,
            recommended_min_batch_max_queued_writes=BATCH_MAX_QUEUED_WRITES_RECOMMENDED_FLOOR,
            suggestion=(
                "Use batch_max_queued_writes=1 for immediate durable writes, "
                f"or {BATCH_MAX_QUEUED_WRITES_RECOMMENDED_FLOOR}+ for useful batching."
            ),
        )

    def _resolve_batch_config_from_config(
        self,
        batch_max_queued_writes: int | None,
        batch_max_flush_delay_ms: int | None,
    ) -> tuple[int | None, int | None]:
        if self._batch_tuning == "manual":
            resolved_batch_max_queued_writes = (
                DEFAULT_BATCH_MAX_QUEUED_WRITES
                if batch_max_queued_writes is None
                else batch_max_queued_writes
            )
            resolved_batch_max_flush_delay_ms = (
                DEFAULT_BATCH_MAX_FLUSH_DELAY_MS
                if batch_max_flush_delay_ms is None
                else batch_max_flush_delay_ms
            )
            self._validate_batch_config(
                resolved_batch_max_queued_writes,
                resolved_batch_max_flush_delay_ms,
            )
            return resolved_batch_max_queued_writes, resolved_batch_max_flush_delay_ms

        if self._batch_tuning == "calibrated":
            if batch_max_queued_writes is not None or batch_max_flush_delay_ms is not None:
                raise ValueError(
                    "batch_max_queued_writes and batch_max_flush_delay_ms cannot be set when "
                    "batch_tuning='calibrated'"
                )
            return None, None

        raise RuntimeError(f"unexpected batch_tuning: {self._batch_tuning!r}")

    # Measurement Outputs

    def _resolve_batch_config_from_measurements(self, measurements: StartupMeasurements) -> None:
        if self._batch_tuning == "manual":
            return
        if self._batch_tuning != "calibrated":
            raise RuntimeError(f"unexpected batch_tuning: {self._batch_tuning!r}")

        batch_max_queued_writes, batch_max_flush_delay_ms = self._derive_batch_config_from_measurements(
            measurements.commit_p50_ms,
        )
        self._validate_batch_config(
            batch_max_queued_writes,
            batch_max_flush_delay_ms,
        )
        self._batch_max_queued_writes = batch_max_queued_writes
        self._batch_max_flush_delay_ms = batch_max_flush_delay_ms

    def _apply_write_warn_thresholds(self, measurements: StartupMeasurements) -> None:
        self._warn_write_thresholds = self._build_write_warn_thresholds(
            measurements.commit_p50_ms,
            measurements.commit_p95_ms,
        )

    def _build_write_warn_thresholds(
        self,
        commit_p50_ms: float,
        commit_p95_ms: float,
    ) -> WriteWarnThresholds:
        batch_max_queued_writes = self._batch_max_queued_writes
        if batch_max_queued_writes is None:
            raise RuntimeError("batch_max_queued_writes must be resolved before warning config is built")

        warn_commit = max(8.0, commit_p95_ms * 3.0)
        crit_commit = max(12.0, commit_p95_ms * 5.0)
        warn_queued_writes = batch_max_queued_writes * 4
        crit_queued_writes = batch_max_queued_writes * 8
        commit_s = max(0.002, commit_p50_ms / 1000.0)
        est_rps = int((batch_max_queued_writes / commit_s) * 0.7)
        warn_rps = max(300, est_rps)
        crit_rps = int(est_rps * 1.5)

        return WriteWarnThresholds(
            commit_p95_ms=WarnThreshold(warn=warn_commit, crit=crit_commit),
            queued_writes=WarnThreshold(warn=warn_queued_writes, crit=crit_queued_writes),
            rows_per_sec=WarnThreshold(warn=warn_rps, crit=crit_rps),
        )

    async def _log_startup_measurements_summary(self, measurements: StartupMeasurements) -> None:
        await self._mn_logger._log(
            DEBUG,
            f"{type(self).__name__} startup measurement summary",
            p50_commit_ms=round(measurements.commit_p50_ms, 2),
            p95_commit_ms=round(measurements.commit_p95_ms, 2),
            batch_tuning=self._batch_tuning,
            batch_max_queued_writes=self._batch_max_queued_writes,
            batch_max_flush_delay_ms=self._batch_max_flush_delay_ms,
            measurement_result=measurements.result,
            sqlite_page_size=self._sqlite_page_size,
            warn_cfg=self._warn_write_thresholds,
        )

    def _derive_batch_config_from_measurements(self, commit_p50_ms: float) -> tuple[int, int]:
        """return (batch_max_queued_writes, batch_max_flush_delay_ms)"""
        if commit_p50_ms <= COMMIT_P50_MS_MAX_NVME_LIKE:
            return (
                BATCH_MAX_QUEUED_WRITES_NVME_LIKE,
                BATCH_MAX_FLUSH_DELAY_MS_NVME_LIKE,
            )
        if commit_p50_ms <= COMMIT_P50_MS_MAX_SSD_LIKE:
            return (
                BATCH_MAX_QUEUED_WRITES_SSD_LIKE,
                BATCH_MAX_FLUSH_DELAY_MS_SSD_LIKE,
            )
        return (
            BATCH_MAX_QUEUED_WRITES_HDD_LIKE,
            BATCH_MAX_FLUSH_DELAY_MS_HDD_LIKE,
        )

    # Batch Write Engine

    async def _enqueue_batch_entry(
        self,
        workflow_id: str,
        op: Literal["upsert", "delete"],
        orchestration_id: str | None,
        payload: bytes | None,
    ) -> asyncio.Future[None]:
        item = PendingWrite(
            workflow_id=workflow_id,
            op=op,
            orchestration_id=orchestration_id,
            payload=payload,
            completion=asyncio.get_running_loop().create_future(),
        )
        async with self._batch_buffer_lock:
            self._batch_buffer.append(item)
            self._batch_buffer_last_enqueue_at = time.monotonic()
            if self._should_flush_now():
                self._enqueue_to_batch_commit_queue(list(self._batch_buffer))
                self._batch_buffer.clear()
                self._batch_buffer_flush_deadline = None
                self._batch_buffer_last_enqueue_at = None
            else:
                self._ensure_flush_task_locked()
            self._maybe_warn_queued_writes()

        return item.completion

    def _should_flush_now(self) -> bool:
        batch_max_queued_writes = self._batch_max_queued_writes
        if batch_max_queued_writes is None:
            raise RuntimeError("batch_max_queued_writes must be resolved before enqueuing writes")
        return len(self._batch_buffer) >= batch_max_queued_writes

    def _ensure_flush_task_locked(self) -> None:
        if self._batch_buffer_flush_task and not self._batch_buffer_flush_task.done():
            return

        batch_max_flush_delay_ms = self._batch_max_flush_delay_ms
        if batch_max_flush_delay_ms is None:
            raise RuntimeError("batch_max_flush_delay_ms must be resolved before scheduling flushes")

        self._batch_buffer_flush_deadline = time.monotonic() + (batch_max_flush_delay_ms / 1000.0)
        self._batch_buffer_flush_task = safe_create_task(
            self._flush_soon(),
            self._mn_logger,
            name=f"{type(self).__name__}._flush_soon",
        )

    def _next_batch_flush_deadline(self) -> float | None:
        deadline = self._batch_buffer_flush_deadline
        if self._batch_max_interarrival_delay_ms is not None and self._batch_buffer_last_enqueue_at is not None:
            interarrival_deadline = self._batch_buffer_last_enqueue_at + (
                self._batch_max_interarrival_delay_ms / 1000.0
            )
            deadline = interarrival_deadline if deadline is None else min(deadline, interarrival_deadline)
        return deadline

    async def _flush_soon(self) -> None:
        try:
            while True:
                deadline = self._next_batch_flush_deadline()
                delay = 0.0 if deadline is None else max(0.0, deadline - time.monotonic())
                if not delay:
                    break
                await asyncio.sleep(delay)
            self._batch_buffer_flush_task_is_flushing = True
            await self._flush()
        finally:
            async with self._batch_buffer_lock:
                self._batch_buffer_flush_task_is_flushing = False
                if self._batch_buffer and self._batch_buffer_flush_task is asyncio.current_task():
                    self._batch_buffer_flush_task = None
                    self._ensure_flush_task_locked()

    async def _flush(self) -> None:
        items = await self._drain_batch_buffer()
        if items:
            self._enqueue_to_batch_commit_queue(items)
        await self._await_batch_commit_queue_drained()

    async def _drain_batch_buffer(self) -> list[PendingWrite]:
        async with self._batch_buffer_lock:
            if not self._batch_buffer:
                return []
            items = list(self._batch_buffer)
            self._batch_buffer.clear()
            self._batch_buffer_flush_deadline = None
            self._batch_buffer_last_enqueue_at = None
            return items

    def _enqueue_to_batch_commit_queue(self, items: list[PendingWrite]) -> None:
        self._batch_commit_queue_pending_writes += len(items)
        self._batch_commit_queue.append(items)
        if self._batch_commit_queue_worker_task is None or self._batch_commit_queue_worker_task.done():
            self._batch_commit_queue_worker_task = asyncio.create_task(
                self._run_batch_commit_queue(),
                name=f"{type(self).__name__}._run_batch_commit_queue",
            )

    async def _run_batch_commit_queue(self) -> None:
        while self._batch_commit_queue:
            items = self._batch_commit_queue.popleft()
            requeued = False
            try:
                await self._commit_batch_and_settle(items)
            except asyncio.CancelledError:
                self._batch_commit_queue.appendleft(items)
                requeued = True
                raise
            except BaseException as e:
                try:
                    await self._mn_logger._log(
                        ERROR,
                        f"{type(self).__name__} commit queue failed to commit batch",
                        error_type=type(e).__name__,
                        error_message=str(e),
                        traceback=format_exception_traceback(e),
                    )
                except Exception:
                    pass
            finally:
                if not requeued:
                    self._batch_commit_queue_pending_writes -= len(items)

    async def _await_batch_commit_queue_drained(self) -> None:
        while self._batch_commit_queue_pending_writes:
            worker = self._batch_commit_queue_worker_task
            if worker is None:
                if not self._batch_commit_queue:
                    raise RuntimeError("batch commit queue has pending writes without a worker task")
                worker = asyncio.create_task(
                    self._run_batch_commit_queue(),
                    name=f"{type(self).__name__}._run_batch_commit_queue",
                )
                self._batch_commit_queue_worker_task = worker
            await asyncio.shield(worker)

    async def _commit_batch_and_settle(self, items: list[PendingWrite]) -> None:
        async with self._batch_commit_execution_lock:
            try:
                dt_ms = await self._commit_batch_now(items)
                self._record_commit_metrics(dt_ms, len(items))
            except BaseException as e:
                for item in items:
                    if not item.completion.done():
                        item.completion.set_exception(e)
                raise

            for item in items:
                if not item.completion.done():
                    item.completion.set_result(None)

    async def _commit_batch_now(self, items: list[PendingWrite]) -> float:
        db = self._require_db()
        t0 = time.perf_counter()
        transaction_started = False
        try:
            await db.execute("BEGIN IMMEDIATE")
            transaction_started = True
            for item in items:
                if item.op == "upsert":
                    if item.orchestration_id is None or item.payload is None:
                        raise RuntimeError("upsert batch item must include orchestration_id and payload")
                    await db.execute(
                        SQL_WORKFLOW_UPSERT,
                        (item.workflow_id, item.orchestration_id, item.payload),
                    )
                elif item.op == "delete":
                    await db.execute(SQL_WORKFLOW_DELETE, (item.workflow_id,))
                else:
                    raise RuntimeError(f"unexpected batch operation: {item.op!r}")
            await db.commit()
            transaction_started = False
            return (time.perf_counter() - t0) * 1000.0
        except BaseException as commit_error:
            if transaction_started:
                try:
                    await db.rollback()
                except Exception as rollback_error:
                    try:
                        await self._mn_logger._log(
                            ERROR,
                            f"{type(self).__name__} failed to rollback batch transaction after commit error",
                            error_type=type(rollback_error).__name__,
                            error_message=str(rollback_error),
                            traceback=format_exception_traceback(rollback_error),
                            commit_error_type=type(commit_error).__name__,
                            commit_error_message=str(commit_error),
                        )
                    except Exception:
                        pass
            raise

    # Telemetry and Warnings

    def _record_commit_metrics(self, dt_ms: float, row_count: int) -> None:
        self._metric_commit_latency_ms_hist.append(dt_ms)

        now_s = int(time.monotonic())
        if now_s != self._metric_rows_per_sec_window_ts:
            self._metric_rows_per_sec_hist.append(self._metric_rows_per_sec_window_rows)
            self._metric_rows_per_sec_window_ts, self._metric_rows_per_sec_window_rows = now_s, 0
        self._metric_rows_per_sec_window_rows += row_count

        self._check_perf_signals()

    def _check_perf_signals(self) -> None:
        if len(self._metric_commit_latency_ms_hist) >= 20:
            p95 = statistics.quantiles(
                self._metric_commit_latency_ms_hist,
                n=20,
                method="inclusive",
            )[-1]
            commit_threshold = self._warn_write_thresholds.commit_p95_ms
            if p95 > commit_threshold.warn:
                self._maybe_warn_write_pressure(
                    f"commit_p95={p95:.1f}ms>warn({commit_threshold.warn:.1f}ms)",
                    critical=p95 > commit_threshold.crit,
                )

        if self._metric_rows_per_sec_hist:
            avg_rps = sum(self._metric_rows_per_sec_hist) / max(1, len(self._metric_rows_per_sec_hist))
            rows_per_sec_threshold = self._warn_write_thresholds.rows_per_sec
            if avg_rps > rows_per_sec_threshold.warn:
                self._maybe_warn_write_pressure(
                    f"rows_per_sec={avg_rps:.0f}>warn({rows_per_sec_threshold.warn})",
                    critical=avg_rps > rows_per_sec_threshold.crit,
                )

    def _maybe_warn_queued_writes(self, *, extra_queued_writes: int = 0) -> None:
        queued_writes_threshold = self._warn_write_thresholds.queued_writes
        queued_writes = len(self._batch_buffer) + self._batch_commit_queue_pending_writes + extra_queued_writes
        if queued_writes > queued_writes_threshold.warn:
            self._maybe_warn_write_pressure(
                f"queued_writes={queued_writes}>warn({queued_writes_threshold.warn})",
                critical=queued_writes > queued_writes_threshold.crit,
            )

    def _maybe_warn_write_pressure(self, reason: str, *, critical: bool = False) -> None:
        now = time.monotonic()
        in_cooldown = now - self._warn_write_last_at < self._warn_write_cooldown_s
        escalating_to_critical = critical and not self._warn_write_last_critical
        if in_cooldown and not escalating_to_critical:
            return
        self._warn_write_last_at = now
        self._warn_write_last_critical = critical
        level = CRITICAL if critical else WARNING
        level_name = "CRITICAL" if critical else "WARN"
        msg = (
            f"{type(self).__name__}: write pressure [{level_name}] ({reason}). "
            "Consider: increase batch caps; confirm WAL + synchronous=NORMAL; "
            "move to a stronger state store if sustained."
        )
        safe_create_task(self._mn_logger._log(level, msg))

    def _maybe_warn_large_payload(
        self,
        workflow_id: str,
        orchestration_id: str | None,
        context_blob: bytes,
    ) -> None:
        sqlite_page_size = self._sqlite_page_size
        if sqlite_page_size is None:
            return

        size = len(context_blob)
        pages = (size + sqlite_page_size - 1) // sqlite_page_size
        last_warn_ts = self._warn_size_last_at_by_workflow_id.get(workflow_id)
        now = time.monotonic()

        if pages >= self._warn_size_pages and (
            last_warn_ts is None or (now - last_warn_ts) > self._warn_size_cooldown_s
        ):
            level = CRITICAL if pages >= self._warn_size_crit_pages else WARNING
            warn_kib = (self._warn_size_pages * sqlite_page_size) // 1024
            crit_kib = (self._warn_size_crit_pages * sqlite_page_size) // 1024
            safe_create_task(
                self._mn_logger._log(
                    level,
                    f"{type(self).__name__}: Large workflow context blob detected",
                    size_bytes=size,
                    approx_pages=pages,
                    workflow_id=workflow_id,
                    orchestration_id=orchestration_id,
                    suggestion=(
                        "Consider externalizing large blobs and storing refs; keep state below "
                        f"configured warning threshold (~{warn_kib}KiB, critical ~{crit_kib}KiB)."
                    ),
                )
            )
            self._warn_size_last_at_by_workflow_id[workflow_id] = now
