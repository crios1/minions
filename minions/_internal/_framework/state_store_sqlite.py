import asyncio
import aiosqlite
import statistics
import time
from collections import deque
from typing import Literal, Tuple

from .logger import Logger, DEBUG, ERROR, WARNING, CRITICAL
from .state_store import StateStore, StoredWorkflowContext
from .._utils.format_exception_traceback import format_exception_traceback
from .._utils.safe_create_task import safe_create_task

BatchEntry = tuple[str, Literal["upsert", "delete"], str | None, bytes | None, asyncio.Future[None]]


class SQLiteStateStore(StateStore):
    """
    SQLite-backed state store for workflow contexts.

    Design Goals
    ------------
    - **Single long-lived connection** tuned with WAL + synchronous=NORMAL
    - **BLOB storage** (binary-encoded payloads via `msgspec`)
    - **Micro-batching**:
        * Buffer up to `batch_max_n` contexts or `batch_max_ms` elapsed time
    - **Boot calibration**:
        * Measure median/p95 commit latency
        * Pick sensible defaults for batch size/window based on disk speed
          (NVMe → small fast batches, HDD → larger slower batches)
    - **Hardware-relative warnings**:
        * Detect when system is under more load than SQLite can sustain
        * Thresholds scale with measured baseline so they work across NVMe/SSD/HDD

    Why batching?
    -------------
    - Without batching: every state update → its own transaction/fsync
      = high overhead, quickly I/O-bound.
    - With batching: amortize commit cost over many updates
      = 3-10x throughput increase depending on hardware.

    Warning Signals
    ---------------
    Warnings are not "your code is broken," they're "SQLite can't keep up anymore."
    They help decide when to tune caps, use a stronger store, or shard work across minion runtimes.

    - **Commit p95 (ms)**:
        Fires if live p95 commit time > ~3x calibrated baseline.
        Indicates storage slowness or single-writer contention.
    - **Backlog (rows)**:
        Fires if in-memory buffer grows beyond ~4x batch_max_n.
        Indicates producers are faster than SQLite can _flush.
        Only visible with batching.
    - **Rows/sec capacity**:
        Fires if sustained write rate exceeds calibrated device capacity.
        Indicates you're outgrowing SQLite's throughput ceiling.

    Operator Actions
    ----------------
    1. If latency budget allows → increase batch caps (`batch_max_n`, `batch_max_ms`)
    2. Confirm WAL + synchronous=NORMAL, consider adjusting wal_autocheckpoint

    Summary
    -------
    - Startup calibration improves performance over per-event commits
    - Micro-batching improves throughput
    - Warnings give clear "you're outgrowing SQLite" signals, scaled to hardware
    """

    def __init__(
        self,
        db_path: str,
        logger: Logger,
        *,
        batch_max_n: int | None = None,
        batch_max_ms: int | None = None,
        journal_mode: str = "WAL",
        synchronous: str = "NORMAL",
        wal_autocheckpoint: int = 1000,
        busy_timeout_ms: int = 3000,
        warn_cooldown_s: float = 30.0,
        size_warn_pages: int = 32,
        size_crit_pages: int = 256,
        size_warn_cooldown_s: float = 3600.0,
    ):
        super().__init__(logger)
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

        self._batch: list[BatchEntry] = []
        self._lock = asyncio.Lock()
        self._commit_lock = asyncio.Lock()
        self._flush_task: asyncio.Task | None = None
        self._deadline: float | None = None

        self._batch_max_n = batch_max_n
        self._batch_max_ms = batch_max_ms
        self._min_n, self._max_n = 16, 256
        self._min_ms, self._max_ms = 5, 40

        self._commit_ms_hist: deque[float] = deque(maxlen=200)
        self._rows_sec_hist: deque[int] = deque(maxlen=60)
        self._sec_bucket_ts = int(time.monotonic())
        self._sec_bucket_rows = 0
        self._warn_cfg: dict[str, Tuple[float, float]] = {}
        self._last_warn_ts = 0.0
        self._warn_cooldown_s = warn_cooldown_s

        self._page_size = 4096
        self._size_warn_pages = size_warn_pages
        self._size_crit_pages = size_crit_pages
        self._last_size_warn: dict[str, float] = {}
        self._size_warn_cooldown_s = size_warn_cooldown_s

        self._wal_autocheckpoint = wal_autocheckpoint
        self._busy_timeout_ms = busy_timeout_ms
        self._journal_mode = journal_mode
        self._synchronous = synchronous

    async def startup(self) -> None:

        #  setup db

        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute(f"PRAGMA journal_mode={self._journal_mode}")
        await self._db.execute(f"PRAGMA synchronous={self._synchronous}")
        await self._db.execute(f"PRAGMA wal_autocheckpoint={self._wal_autocheckpoint}")
        await self._db.execute(f"PRAGMA busy_timeout={self._busy_timeout_ms}")
        # startup assumes latest schema; add migrations here if schema evolves (e.g. PRAGMA user_version).
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS workflows(
                workflow_id TEXT PRIMARY KEY,
                orchestration_id TEXT NOT NULL,
                context BLOB NOT NULL
            )
        """)
        await self._db.execute("""
            CREATE INDEX IF NOT EXISTS idx_workflows_orchestration_id
            ON workflows(orchestration_id)
        """)
        await self._db.commit()
        async with self._db.execute("PRAGMA page_size") as c:
            row = await c.fetchone()
        if row and row[0]:
            self._page_size = int(row[0])


        # calibrate writer

        await self._db.executescript("BEGIN IMMEDIATE; ROLLBACK; " * 6)

        try:
            async def run_probe(payload: bytes) -> list[float]:
                samples = []
                calib_id = "__state_store_calib__"
                calib_orchestration_id = "__state_store_calib__"
                # ensure the row exists (won't grow table further)
                await self._db.execute( # type: ignore
                    """INSERT INTO workflows(workflow_id, orchestration_id, context)
                    VALUES(?, ?, ?)
                    ON CONFLICT(workflow_id) DO NOTHING""",
                    (calib_id, calib_orchestration_id, payload),
                )
                await self._db.commit() # type: ignore
                for _ in range(8):
                    t = time.perf_counter()
                    await self._db.execute("BEGIN IMMEDIATE") # type: ignore
                    await self._db.execute( # type: ignore
                        """INSERT INTO workflows(workflow_id, orchestration_id, context)
                        VALUES(?, ?, ?)
                        ON CONFLICT(workflow_id) DO UPDATE SET
                            orchestration_id=excluded.orchestration_id,
                            context=excluded.context""",
                        (calib_id, calib_orchestration_id, payload),
                    )
                    await self._db.commit() # type: ignore
                    samples.append((time.perf_counter() - t) * 1000.0)
                return samples

            smalls = await run_probe(b"x" * 256)   # ~pure fsync baseline
            mediums = await run_probe(b"x" * (self._page_size * 2)) # ~2 WAL pages

            samples = smalls + mediums
            p50 = statistics.median(samples)
            p95 = statistics.quantiles(samples, n=20, method="inclusive")[-1]
        except Exception as e:
            p50, p95 = 6.0, 10.0  # SSD-ish fallback
            await self._mn_logger._log(
                ERROR,
                f"{type(self).__name__} calibration failed; using defaults",
                traceback=format_exception_traceback(e),
            )
        else:
            await self._db.execute(
                "DELETE FROM workflows WHERE workflow_id = ?",
                ("__state_store_calib__",),
            )
            await self._db.commit()

        if (self._batch_max_n is None) or (self._batch_max_ms is None):
            if p50 <= 2.0:
                self._batch_max_n = 48
                self._batch_max_ms = 8
            elif p50 <= 6.0:
                self._batch_max_n = 64
                self._batch_max_ms = 16
            else:
                self._batch_max_n = 128
                self._batch_max_ms = 30

        self._batch_max_n = min(max(self._batch_max_n, self._min_n), self._max_n)
        self._batch_max_ms = min(max(self._batch_max_ms, self._min_ms), self._max_ms)

        warn_commit = max(8.0, p95 * 3.0)
        crit_commit = max(12.0, p95 * 5.0)
        warn_backlog = self._batch_max_n * 4
        crit_backlog = self._batch_max_n * 8
        commit_s = max(0.002, p50 / 1000.0)
        est_rps = int((self._batch_max_n / commit_s) * 0.7)
        warn_rps = max(300, est_rps)
        crit_rps = int(est_rps * 1.5)

        self._warn_cfg = {
            "commit_p95_ms": (warn_commit, crit_commit),
            "backlog_n": (warn_backlog, crit_backlog),
            "rows_per_sec": (warn_rps, crit_rps),
        }

        await self._mn_logger._log(
            DEBUG,
            f"{type(self).__name__} calibrated",
            p50_commit_ms=round(p50, 2),
            p95_commit_ms=round(p95, 2),
            batch_max_n=self._batch_max_n,
            batch_max_ms=self._batch_max_ms,
            warn_cfg=self._warn_cfg
        )

    async def shutdown(self) -> None:
        await self._flush()
        if self._flush_task and not self._flush_task.done():
            await asyncio.gather(self._flush_task, return_exceptions=True)
        self._flush_task = None
        await self._db.close() # type: ignore

    async def _flush(self):
        items: list[BatchEntry] = []
        async with self._lock:
            if not self._batch:
                return
            items = list(self._batch)
            self._batch.clear()
            self._deadline = None
        await self._commit_and_settle(items)

    async def _commit_and_settle(self, items: list[BatchEntry]) -> None:
        async with self._commit_lock:
            try:
                await self._commit_batch_now(items)
            except Exception as e:
                for *_, waiter in items:
                    if not waiter.done():
                        waiter.set_exception(e)
                raise
            for *_, waiter in items:
                if not waiter.done():
                    waiter.set_result(None)

    async def _flush_soon(self):
        delay = 0 if self._deadline is None else max(0.0, self._deadline - time.monotonic())
        if delay:
            await asyncio.sleep(delay)
        await self._flush()

    async def _commit_batch_now(self, items: list[BatchEntry]):
        t0 = time.perf_counter()
        await self._db.execute("BEGIN IMMEDIATE")  # type: ignore
        upserts = [
            (workflow_id, orchestration_id, payload)
            for workflow_id, op, orchestration_id, payload, _waiter in items
            if op == "upsert" and payload is not None
        ]
        deletes = [
            (workflow_id,)
            for workflow_id, op, _orchestration_id, _payload, _waiter in items
            if op == "delete"
        ]
        if upserts:
            await self._db.executemany(  # type: ignore
                """INSERT INTO workflows(workflow_id, orchestration_id, context)
                VALUES(?, ?, ?)
                ON CONFLICT(workflow_id) DO UPDATE SET
                    orchestration_id=excluded.orchestration_id,
                    context=excluded.context""",
                upserts,
            )
        if deletes:
            await self._db.executemany(  # type: ignore
                "DELETE FROM workflows WHERE workflow_id = ?",
                deletes,
            )
        await self._db.commit()  # type: ignore
        dt_ms = (time.perf_counter() - t0) * 1000.0
        self._commit_ms_hist.append(dt_ms)

        now_s = int(time.monotonic())
        if now_s != self._sec_bucket_ts:
            self._rows_sec_hist.append(self._sec_bucket_rows)
            self._sec_bucket_ts, self._sec_bucket_rows = now_s, 0
        self._sec_bucket_rows += len(items)

        self._check_perf_signals()

    def _check_perf_signals(self):
        if len(self._commit_ms_hist) >= 20:
            p95 = statistics.quantiles(self._commit_ms_hist, n=20, method="inclusive")[-1]
            warn, crit = self._warn_cfg.get("commit_p95_ms", (float("inf"), float("inf")))
            if p95 > warn:
                self._maybe_warn_overload(f"commit_p95={p95:.1f}ms>warn({warn:.1f}ms)", critical=p95 > crit)

        if self._rows_sec_hist:
            avg_rps = sum(self._rows_sec_hist) / max(1, len(self._rows_sec_hist))
            warn_rps, crit_rps = self._warn_cfg.get("rows_per_sec", (float("inf"), float("inf")))
            if avg_rps > warn_rps:
                self._maybe_warn_overload(f"rows_per_sec~{avg_rps:.0f}>warn({warn_rps})", critical=avg_rps > crit_rps)

    def _maybe_warn_overload(self, reason: str, *, critical: bool = False):
        now = time.monotonic()
        if now - self._last_warn_ts < self._warn_cooldown_s:
            return
        self._last_warn_ts = now
        level = "CRITICAL" if critical else "WARN"
        msg = (
            f"{type(self).__name__}: storage pressure [{level}] ({reason}). "
            "Consider: increase batch caps; confirm WAL + synchronous=NORMAL; "
            "shard by workflow_id across multiple SQLite DBs; or move to Postgres if sustained."
        )
        safe_create_task(self._mn_logger._log(ERROR, msg))

    def _maybe_warn_large_payload(
        self,
        workflow_id: str,
        orchestration_id: str | None,
        context_blob: bytes,
    ) -> None:
        size = len(context_blob)
        pages = (size + self._page_size - 1) // self._page_size
        last = self._last_size_warn.get(workflow_id)
        now = time.monotonic()
        if pages >= self._size_warn_pages and (
            last is None or (now - last) > self._size_warn_cooldown_s
        ):
            lvl = CRITICAL if pages >= self._size_crit_pages else WARNING
            warn_kib = (self._size_warn_pages * self._page_size) // 1024
            crit_kib = (self._size_crit_pages * self._page_size) // 1024
            safe_create_task(
                self._mn_logger._log(
                    lvl,
                    f"{type(self).__name__}: Large MinionWorkflowContext Detected",
                    size_bytes=size,
                    approx_pages=pages,
                    workflow_id=workflow_id,
                    orchestration_id=orchestration_id,
                    suggestion=(
                        "Consider externalizing large blobs and storing refs; keep state below "
                        f"configured warning threshold (~{warn_kib}KiB, critical ~{crit_kib}KiB)."
                    )
                )
            )
            self._last_size_warn[workflow_id] = now

    async def _enqueue_batch_entry(
        self,
        workflow_id: str,
        op: Literal["upsert", "delete"],
        orchestration_id: str | None,
        payload: bytes | None,
    ) -> asyncio.Future[None]:
        fut = asyncio.get_running_loop().create_future()
        to_flush = False
        flush_items: list[BatchEntry] | None = None
        async with self._lock:
            self._batch.append((workflow_id, op, orchestration_id, payload, fut))
            if len(self._batch) >= self._batch_max_n: # type: ignore
                to_flush = True
                flush_items = list(self._batch)
                self._batch.clear()
                self._deadline = None
            elif not self._flush_task or self._flush_task.done():
                self._deadline = time.monotonic() + (self._batch_max_ms / 1000.0)  # type: ignore
                self._flush_task = safe_create_task(
                    self._flush_soon(),
                    self._mn_logger,
                    name=f"{type(self).__name__}._flush_soon",
                )

            warn_n, crit_n = self._warn_cfg.get("backlog_n", (float("inf"), float("inf")))
            if len(self._batch) > warn_n:
                self._maybe_warn_overload(
                    f"backlog>{warn_n} (cur={len(self._batch)})", critical=len(self._batch) > crit_n
                )
        if to_flush:
            await self._commit_and_settle(flush_items or [])
        return fut

    async def save_context(
        self,
        workflow_id: str,
        orchestration_id: str,
        context: bytes,
    ):
        self._maybe_warn_large_payload(
            workflow_id,
            orchestration_id,
            context,
        )
        fut = await self._enqueue_batch_entry(
            workflow_id,
            "upsert",
            orchestration_id,
            context,
        )
        await fut

    async def delete_context(self, workflow_id: str):
        fut = await self._enqueue_batch_entry(workflow_id, "delete", None, None)
        await fut

    async def get_contexts_for_orchestration(
        self,
        orchestration_id: str,
    ) -> list[StoredWorkflowContext]:
        await self._flush()
        async with self._db.execute(
            """
            SELECT workflow_id, orchestration_id, context
            FROM workflows
            WHERE orchestration_id = ?
            """,
            (orchestration_id,),
        ) as c: # type: ignore
            rows = await c.fetchall()
        return [
            StoredWorkflowContext(
                workflow_id=workflow_id,
                orchestration_id=row_orchestration_id,
                context=bytes(context_blob),
            )
            for workflow_id, row_orchestration_id, context_blob in rows
        ]

    async def get_all_contexts(self) -> list[StoredWorkflowContext]:
        await self._flush()
        async with self._db.execute(
            "SELECT workflow_id, orchestration_id, context FROM workflows"
        ) as c: # type: ignore
            rows = await c.fetchall()
        return [
            StoredWorkflowContext(
                workflow_id=workflow_id,
                orchestration_id=orchestration_id,
                context=bytes(context_blob),
            )
            for workflow_id, orchestration_id, context_blob in rows
        ]
