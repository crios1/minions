from typing import Any

from .logger import Logger

class NoOpLogger(Logger):
    def __init__(self): ...

    async def log(self, level: int, msg: str, **kwargs: Any) -> None: ...