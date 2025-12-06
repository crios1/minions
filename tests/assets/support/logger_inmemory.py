from dataclasses import dataclass
from minions._internal._framework.logger import Logger, DEBUG, INFO, WARNING, ERROR, CRITICAL
from .mixin_spy import SpyMixin

@dataclass
class Log:
    level: int
    msg: str
    kwargs: dict

class InMemoryLogger(SpyMixin, Logger):
    """In-memory implementation of Logger for testing."""

    def __init__(self, level: int = INFO):
        super().__init__(level)
        self.logs: list[Log] = []

    async def log(self, level: int, msg: str, **kwargs):
        self.logs.append(Log(level, msg, kwargs))

    def logged_before(self, substr_1: str, substr_2: str, min_level: int = DEBUG) -> bool:
        def _find_idx(substr: str) -> int | None:
            for i, log in enumerate(self.logs):
                if log.level >= min_level and substr in log.msg:
                    return i
        idx_1 = _find_idx(substr_1)
        idx_2 = _find_idx(substr_2)
        return idx_1 != None and idx_2 != None and idx_1 < idx_2

    def has_log(self, substr: str, min_level: int = DEBUG) -> bool:
        for log in self.logs:
            if log.level >= min_level and substr in log.msg:
                return True
        return False
