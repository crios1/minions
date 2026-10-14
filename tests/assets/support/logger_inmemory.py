import asyncio
from collections.abc import Collection
from dataclasses import dataclass
from typing import Any

from minions._internal._framework.logger import DEBUG, INFO

from .log_contracts import assert_each_log_matches_exactly_one_contract
from .logger_spied import SpiedLogger


@dataclass
class Log:
    level: int
    msg: str
    kwargs: dict[str, Any]


class InMemoryLogger(SpiedLogger):
    """In-memory implementation of Logger for testing."""

    def __init__(self, level: int = INFO) -> None:
        super().__init__(level)
        self.logs: list[Log] = []
        self._log_recorded = asyncio.Condition()

    async def log(self, level: int, msg: str, **kwargs: Any) -> None:
        async with self._log_recorded:
            self.logs.append(Log(level, msg, kwargs))
            self._log_recorded.notify_all()

    def assert_recorded_logs_match_contracts(self) -> None:
        assert_each_log_matches_exactly_one_contract(self.logs)

    def find_first_log(
        self,
        substr: str | None = None,
        min_level: int = DEBUG,
        log_kwargs: dict[str, object] | None = None,
        *,
        log_kwarg_keys: Collection[str] = (),
    ) -> Log | None:
        """Find the first log matching the supplied message and kwarg filters.

        ``substr=None`` omits message filtering. ``log_kwargs`` matches exact values,
        while ``log_kwarg_keys`` checks key presence regardless of value.
        """
        log_kwargs = log_kwargs or {}
        for log in self.logs:
            if (
                log.level >= min_level
                and (substr is None or substr in log.msg)
                and all(k in log.kwargs and log.kwargs[k] == v for k, v in log_kwargs.items())
                and all(k in log.kwargs for k in log_kwarg_keys)
            ):
                return log
        return None

    def has_log(
        self,
        substr: str | None = None,
        min_level: int = DEBUG,
        log_kwargs: dict[str, object] | None = None,
        *,
        log_kwarg_keys: Collection[str] = (),
    ) -> bool:
        """Return whether any recorded log matches the supplied filters."""
        return (
            self.find_first_log(
                substr,
                min_level=min_level,
                log_kwargs=log_kwargs,
                log_kwarg_keys=log_kwarg_keys,
            )
            is not None
        )

    async def wait_for_log(
        self,
        substr: str,
        timeout: float = 0.5,
        min_level: int = DEBUG,
        log_kwargs: dict[str, object] | None = None,
    ) -> bool:
        def matching_log_exists() -> bool:
            return self.has_log(substr, min_level=min_level, log_kwargs=log_kwargs)

        if matching_log_exists():
            return True

        try:
            async with asyncio.timeout(timeout):
                async with self._log_recorded:
                    await self._log_recorded.wait_for(matching_log_exists)
        except TimeoutError:
            return False
        return True

    def logged_before(self, substr_1: str, substr_2: str, min_level: int = DEBUG) -> bool:
        def _find_idx(substr: str) -> int | None:
            for i, log in enumerate(self.logs):
                if log.level >= min_level and substr in log.msg:
                    return i
        idx_1 = _find_idx(substr_1)
        idx_2 = _find_idx(substr_2)
        return idx_1 is not None and idx_2 is not None and idx_1 < idx_2
