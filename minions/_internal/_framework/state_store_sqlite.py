import asyncio
import aiosqlite
import importlib
import statistics
import time
import traceback
from collections import deque
from typing import List, Tuple

from .logger import Logger, DEBUG, ERROR, WARNING, CRITICAL
from .state_store import StateStore
from .._domain.minion_workflow_context import MinionWorkflowContext
from .._utils.serialization import serialize, deserialize


def serialize_type(t: type) -> str:
    return f"{t.__module__}.{t.__qualname__}"

def deserialize_type(s: str) -> type:
    module, _, cls = s.rpartition(".")
    return getattr(importlib.import_module(module), cls)

def serialize_context(ctx: MinionWorkflowContext) -> bytes:
    d = ctx.as_dict()
    d["context_cls"] = serialize_type(ctx.context_cls)
    return serialize(d)

def deserialize_context(b: bytes) -> MinionWorkflowContext:
    # decode to a raw dict first, then reconstruct MinionWorkflowContext
    d = deserialize(b, dict)
    t = d.get("context_cls")
    if isinstance(t, str):
        d["context_cls"] = deserialize_type(t)
    return MinionWorkflowContext(**d)


class SQLiteStateStore(StateStore):
    """
    SQLite-backed state store for workflow contexts.

    Design Goals
    ------------
    - **Single long-lived connection** tuned with WAL + synchronous=NORMAL
    - **BLOB storage** (binary-encoded JSON)
    - **Micro-batching** with coalescing:
        * Buffer up to `batch_max_n` contexts or `batch_max_ms` elapsed time
        * Coalesce by workflow_id (last-write-wins within batch)
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
    They help decide when to tune caps or move to a stronger store (e.g. Postgres).

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
    3. Reduce upstream write frequency (coalesce, avoid writing unchanged state)
    4. Shard workflows across multiple SQLite DB files
    5. Migrate to Postgres when sustained load keeps tripping warnings

    Summary
    -------
    - Startup calibration improves performance over naïve per-event commits
    - Micro-batching provides both performance and observability
    - Warnings give clear "you're outgrowing SQLite" signals, scaled to hardware
    """

    def __init__(
        self,
        db_path: str,
        logger: Logger,
        *,
        batch_max_n: int | None = None,
        batch_max_ms: int | None = None,
    ):
        super().__init__(logger)
        self.db_path = db_path
        self._db: aiosqlite.Connection | None = None

        self._batch: dict[str, bytes] = {}
        self._lock = asyncio.Lock()
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
        self._warn_cooldown_s = 30

        self._page_size = 4096
        self._size_warn_pages = 32    # ~128KiB @ 4KiB pages
        self._size_crit_pages = 256   # ~1MiB @ 4KiB pages
        # self._last_size_warn: dict[tuple[str, str], float] = {}
        self._last_size_warn: dict[str, float] = {}
        self._size_warn_cooldown_s = 3600

    async def startup(self):

        #  setup db

        self._db = await aiosqlite.connect(self.db_path)
        await self._db.execute("PRAGMA journal_mode=WAL")
        await self._db.execute("PRAGMA synchronous=NORMAL")
        await self._db.execute("PRAGMA wal_autocheckpoint=1000")
        await self._db.execute("PRAGMA busy_timeout=3000")
        await self._db.execute("""
            CREATE TABLE IF NOT EXISTS workflows(
                workflow_id TEXT PRIMARY KEY,
                context_json BLOB NOT NULL
            )
        """)
        await self._db.commit()


        # calibrate writer

        await self._db.executescript("BEGIN IMMEDIATE; ROLLBACK; " * 6)

        try:
            async def run_probe(payload: bytes) -> list[float]:
                samples = []
                calib_id = "__state_store_calib__"
                # ensure the row exists (won't grow table further)
                await self._db.execute( # type: ignore
                    """INSERT INTO workflows(workflow_id, context_json)
                    VALUES(?, ?)
                    ON CONFLICT(workflow_id) DO NOTHING""",
                    (calib_id, payload),
                )
                await self._db.commit() # type: ignore
                for _ in range(8):
                    t = time.perf_counter()
                    await self._db.execute("BEGIN IMMEDIATE") # type: ignore
                    await self._db.execute( # type: ignore
                        """INSERT INTO workflows(workflow_id, context_json)
                        VALUES(?, ?)
                        ON CONFLICT(workflow_id) DO UPDATE SET context_json=excluded.context_json""",
                        (calib_id, payload),
                    )
                    await self._db.commit() # type: ignore
                    samples.append((time.perf_counter() - t) * 1000.0)
                return samples

            smalls = await run_probe(b"x" * 256)   # ~pure fsync baseline
            mediums = await run_probe(b"x" * 8192) # ~2 WAL pages (4 KiB pages assumed)

            samples = smalls + mediums
            p50 = statistics.median(samples)
            p95 = statistics.quantiles(samples, n=20, method="inclusive")[-1]
        except Exception:
            p50, p95 = 6.0, 10.0  # SSD-ish fallback
            await self._mn_logger._log(
                ERROR,
                f"{type(self).__name__} calibration failed; using defaults",
                traceback="".join(traceback.format_exc()),
            )

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

        async with self._db.execute("PRAGMA page_size") as c:  # type: ignore
            row = await c.fetchone()
        if row and row[0]:
            self._page_size = int(row[0])

    async def shutdown(self):
        await self._flush()
        if self._flush_task and not self._flush_task.done():
            try:
                self._flush_task.cancel()
            except Exception:
                pass
        await self._db.close() # type: ignore

    async def _flush(self):
        async with self._lock:
            if not self._batch:
                return
            items = list(self._batch.items())
            self._batch.clear()
            self._deadline = None
        await self._upsert_now(items)

    async def _flush_soon(self):
        delay = 0 if self._deadline is None else max(0.0, self._deadline - time.monotonic())
        if delay:
            await asyncio.sleep(delay)
        await self._flush()

    async def _upsert_now(self, items: list[tuple[str, bytes]]):
        t0 = time.perf_counter()
        await self._db.execute("BEGIN IMMEDIATE")  # type: ignore
        await self._db.executemany(  # type: ignore
            """INSERT INTO workflows(workflow_id, context_json)
            VALUES(?, ?)
            ON CONFLICT(workflow_id) DO UPDATE SET context_json=excluded.context_json""",
            items,
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
        asyncio.create_task(self._mn_logger._log(ERROR, msg))

    async def save_context(self, ctx: MinionWorkflowContext):
        try:
            payload = serialize_context(ctx)

            size = len(payload)
            pages = (size + self._page_size - 1) // self._page_size
            last = self._last_size_warn.get(ctx.workflow_id, 0.0)
            now = time.monotonic()
            if pages >= self._size_warn_pages and (now - last) > self._size_warn_cooldown_s:
                lvl = CRITICAL if pages >= self._size_crit_pages else WARNING
                asyncio.create_task(self._mn_logger._log(
                    lvl,
                    f"{type(self).__name__}: Large MinionWorkflowContext Detected",
                    size_bytes=size,
                    approx_pages=pages,
                    workflow_id=ctx.workflow_id,
                    minion_modpath=ctx.minion_modpath,
                    suggestion="Consider externalizing large blobs and storing refs; keep state <~128KiB."
                ))
                self._last_size_warn[ctx.workflow_id] = now
        except Exception as e:
            await self._mn_logger._log(
                ERROR, f"{type(self).__name__}.save_context serialize failed",
                error_type=type(e).__name__, error_message=str(e),
                traceback="".join(traceback.format_exception(type(e), e, e.__traceback__)),
                context_id=getattr(ctx, "workflow_id", None),
            )
            return

        to_flush = None
        async with self._lock:
            self._batch[ctx.workflow_id] = payload
            if len(self._batch) >= self._batch_max_n: # type: ignore
                to_flush = list(self._batch.items())
                self._batch.clear()
                self._deadline = None
            elif not self._flush_task or self._flush_task.done():
                self._deadline = time.monotonic() + (self._batch_max_ms / 1000.0)  # type: ignore
                self._flush_task = asyncio.create_task(self._flush_soon())

            warn_n, crit_n = self._warn_cfg.get("backlog_n", (float("inf"), float("inf")))
            if len(self._batch) > warn_n:
                self._maybe_warn_overload(
                    f"backlog>{warn_n} (cur={len(self._batch)})", critical=len(self._batch) > crit_n
                )
        
        if to_flush:
            await self._upsert_now(to_flush)

    async def delete_context(self, workflow_id: str):
        async with self._lock:
            self._batch.pop(workflow_id, None)
        await self._db.execute("DELETE FROM workflows WHERE workflow_id = ?", (workflow_id,))  # type: ignore
        await self._db.commit() # type: ignore

    async def get_all_contexts(self) -> List[MinionWorkflowContext]:
        await self._flush()
        async with self._db.execute("SELECT context_json FROM workflows") as c: # type: ignore
            rows = await c.fetchall()
        if not rows:
            return []
        out: List[MinionWorkflowContext] = []
        for (blob,) in rows:
            try:
                out.append(deserialize_context(blob))
            except Exception as e:
                await self._mn_logger._log(
                    ERROR, f"{type(self).__name__}.get_all_contexts deserialize failed",
                    error_type=type(e).__name__,
                    error_message=str(e),
                    traceback="".join(traceback.format_exception(type(e), e, e.__traceback__))
                )
        return out
