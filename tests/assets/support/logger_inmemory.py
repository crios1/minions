from dataclasses import dataclass
import asyncio
from minions._internal._framework.logger import DEBUG, INFO

from .logger_spied import SpiedLogger

@dataclass
class Log:
    level: int
    msg: str
    kwargs: dict

class InMemoryLogger(SpiedLogger):
    """In-memory implementation of Logger for testing."""

    def __init__(self, level: int = INFO):
        super().__init__(level)
        self.logs: list[Log] = []

    async def log(self, level: int, msg: str, **kwargs):
        self.logs.append(Log(level, msg, kwargs))

    def has_log(
        self,
        substr: str,
        min_level: int = DEBUG,
        log_kwargs: dict[str, object] | None = None,
    ) -> bool:
        log_kwargs = log_kwargs or {}
        for log in self.logs:
            if (
                log.level >= min_level
                and substr in log.msg
                and all(k in log.kwargs and log.kwargs[k] == v for k, v in log_kwargs.items())
            ):
                return True
        return False

    async def wait_for_log(
        self,
        substr: str,
        timeout: float = 0.5,
        min_level: int = DEBUG,
        poll_interval: float = 0.005,
        log_kwargs: dict[str, object] | None = None,
    ) -> bool:
        deadline = asyncio.get_running_loop().time() + timeout
        while asyncio.get_running_loop().time() < deadline:
            if self.has_log(substr, min_level=min_level, log_kwargs=log_kwargs):
                return True
            await asyncio.sleep(poll_interval)
        return False

    def logged_before(self, substr_1: str, substr_2: str, min_level: int = DEBUG) -> bool:
        def _find_idx(substr: str) -> int | None:
            for i, log in enumerate(self.logs):
                if log.level >= min_level and substr in log.msg:
                    return i
        idx_1 = _find_idx(substr_1)
        idx_2 = _find_idx(substr_2)
        return idx_1 is not None and idx_2 is not None and idx_1 < idx_2
