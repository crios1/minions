import inspect
import os
from collections.abc import AsyncGenerator, Awaitable
from dataclasses import dataclass
from typing import Literal, Protocol, get_type_hints

import pytest_asyncio

from minions._internal._framework.state_store_sqlite import SQLiteStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger


StoreAndLogger = tuple[SQLiteStateStore, InMemoryLogger]
BatchTuning = Literal["manual", "calibrated"]


@dataclass(frozen=True)
class _KwargSnapshot:
    annotation: object
    default: object


_SQLITE_STATE_STORE_FACTORY_KWARG_SNAPSHOT = {
    "batch_tuning": _KwargSnapshot(annotation=BatchTuning, default="manual"),
    "batch_max_queued_writes": _KwargSnapshot(annotation=int | None, default=None),
    "batch_max_flush_delay_ms": _KwargSnapshot(annotation=int | None, default=None),
    "batch_max_interarrival_delay_ms": _KwargSnapshot(annotation=int | None, default=None),
    "sqlite_journal_mode": _KwargSnapshot(annotation=str, default="WAL"),
    "sqlite_synchronous": _KwargSnapshot(annotation=str, default="NORMAL"),
    "sqlite_wal_autocheckpoint": _KwargSnapshot(annotation=int, default=1000),
    "sqlite_busy_timeout_ms": _KwargSnapshot(annotation=int, default=3000),
    "warn_size_pages": _KwargSnapshot(annotation=int, default=32),
    "warn_size_crit_pages": _KwargSnapshot(annotation=int, default=256),
    "warn_size_cooldown_s": _KwargSnapshot(annotation=float, default=3600.0),
    "warn_write_cooldown_s": _KwargSnapshot(annotation=float, default=30.0),
}


def _assert_factory_kwargs_match_state_store_constructor() -> None:
    signature = inspect.signature(SQLiteStateStore.__init__)
    type_hints = get_type_hints(SQLiteStateStore.__init__)
    constructor_kwargs = {
        name
        for name, parameter in signature.parameters.items()
        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
    }
    snapshot_kwargs = set(_SQLITE_STATE_STORE_FACTORY_KWARG_SNAPSHOT)

    mismatches: list[str] = []
    for name in sorted(constructor_kwargs - snapshot_kwargs):
        mismatches.append(f"{name}: missing from make_state_store_and_logger kwarg snapshot")
    for name in sorted(snapshot_kwargs - constructor_kwargs):
        mismatches.append(f"{name}: not present as a keyword-only SQLiteStateStore.__init__ parameter")

    for name, snapshot in _SQLITE_STATE_STORE_FACTORY_KWARG_SNAPSHOT.items():
        parameter = signature.parameters.get(name)
        if parameter is None:
            mismatches.append(f"{name}: missing from SQLiteStateStore.__init__")
            continue

        if parameter.default != snapshot.default:
            mismatches.append(
                f"{name}: default is {parameter.default!r}, expected {snapshot.default!r}"
            )

        annotation = type_hints.get(name)
        if annotation != snapshot.annotation:
            mismatches.append(
                f"{name}: annotation is {annotation!r}, expected {snapshot.annotation!r}"
            )

    if mismatches:
        mismatch_text = "\n".join(f"- {mismatch}" for mismatch in mismatches)
        raise AssertionError(
            "make_state_store_and_logger kwarg snapshot drifted from "
            f"SQLiteStateStore.__init__:\n{mismatch_text}"
        )


class MakeStateStoreAndLogger(Protocol):
    def __call__(
        self,
        *,
        db_path: str | None = None,
        logger: InMemoryLogger | None = None,
        batch_tuning: BatchTuning = "manual",
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
    ) -> Awaitable[StoreAndLogger]: ...


@pytest_asyncio.fixture
async def make_state_store_and_logger(
    tmp_path,
) -> AsyncGenerator[MakeStateStoreAndLogger, None]:
    _assert_factory_kwargs_match_state_store_constructor()
    store_loggers: list[StoreAndLogger] = []

    async def make_state_store_and_logger(
        *,
        db_path: str | None = None,
        logger: InMemoryLogger | None = None,
        batch_tuning: BatchTuning = "manual",
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
    ) -> StoreAndLogger:
        if db_path is None:
            db_path = os.path.join(tmp_path, f"state-{len(store_loggers)}.db")
        if logger is None:
            logger = InMemoryLogger()

        store = SQLiteStateStore(
            db_path=db_path,
            logger=logger,
            batch_tuning=batch_tuning,
            batch_max_queued_writes=batch_max_queued_writes,
            batch_max_flush_delay_ms=batch_max_flush_delay_ms,
            batch_max_interarrival_delay_ms=batch_max_interarrival_delay_ms,
            sqlite_journal_mode=sqlite_journal_mode,
            sqlite_synchronous=sqlite_synchronous,
            sqlite_wal_autocheckpoint=sqlite_wal_autocheckpoint,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
            warn_size_pages=warn_size_pages,
            warn_size_crit_pages=warn_size_crit_pages,
            warn_size_cooldown_s=warn_size_cooldown_s,
            warn_write_cooldown_s=warn_write_cooldown_s,
        )

        await logger._mn_startup()
        try:
            await store._mn_startup()
        except Exception:
            await logger._mn_shutdown()
            raise

        store_and_logger = (store, logger)
        store_loggers.append(store_and_logger)
        return store_and_logger

    try:
        yield make_state_store_and_logger
    finally:
        for store, logger in reversed(store_loggers):
            if store._db is not None:
                await store._mn_shutdown()
            await logger._mn_shutdown()
