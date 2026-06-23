from typing import Any

from tests.assets.crash.boom import boom
from tests.assets.support.logger_inmemory import InMemoryLogger


class AssetLogger(InMemoryLogger):
    async def log(self, level: int, msg: str, **kwargs: Any) -> None:
        boom()
