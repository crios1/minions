import sys
from typing import Any
from datetime import datetime, timezone

from .logger import Logger

class ConsoleLogger(Logger):
    def __init__(self): pass

    async def log(self, level: int, msg: str, **kwargs: Any) -> None:
        now = datetime.now(timezone.utc).isoformat()
        print(f"[{now}] [level={level}] {msg} {kwargs}", file=sys.stdout)
