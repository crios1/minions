import sys
from abc import abstractmethod
from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any

from .async_lifecycle import AsyncLifecycle
from .._utils.format_exception_traceback import format_exception_traceback

DEBUG = 10
INFO = 20
WARNING = 30
ERROR = 40
CRITICAL = 50

LEVEL_NAMES = {
    10: "DEBUG",
    20: "INFO",
    30: "WARNING",
    40: "ERROR",
    50: "CRITICAL",
}

class Logger(AsyncLifecycle):
    """
    This framework enforces structured logs by default
    because it is the only sane way to trace workflow behavior, errors, and performance
    in something that behaves like a distributed system.
    """
    _mn_user_facing = True

    def __init__(self, level: int = INFO):
        self._level = level
    
    def _mn_iso_8601_ts(self) -> str:
        "Returns ISO 8601 timestamp like '2025:07:03T20:45:01Z'"
        now_utc = datetime.now(timezone.utc)
        return now_utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    
    def _mn_iso_8601_ts_fs_safe(self) -> str:
        "Returns filesystem-safe ISO 8601 timestamp like '2025-07-03T20-45-01Z'"
        return self._mn_iso_8601_ts().replace(":", "-")

    async def _mn_log(self, level: int, msg: str, **kwargs: Any):
        try:
            return await self.log(level, msg, **kwargs)
        except Exception as e:
            print(f"[Logger Error] {type(e).__name__}: {e}", file=sys.stderr)
            print(f"[Logger Fallback] {msg} | {kwargs}", file=sys.stderr)

    async def _mn_log_exception(
        self,
        level: int,
        msg: str,
        exc: BaseException,
        **kwargs: Any,
    ):
        context = getattr(exc, "context", {}) or {}
        log_kwargs = dict(context) if isinstance(context, Mapping) else {}
        log_kwargs.update(kwargs)
        trace = getattr(exc, "__cause__", None) or exc
        log_kwargs.update(
            {
                "error_type": type(trace).__name__,
                "error_message": str(trace),
                "traceback": format_exception_traceback(trace),
            }
        )

        await self._mn_log(
            level,
            msg,
            **log_kwargs,
        )

    @abstractmethod
    async def log(self, level: int, msg: str, **kwargs: Any):
        "override to implement your logger, kwargs are for inclusion in structured logs"
