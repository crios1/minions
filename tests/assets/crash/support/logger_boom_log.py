from tests.assets.crash.boom import boom
from tests.assets.support.logger_inmemory import InMemoryLogger


class BoomLogger(InMemoryLogger):
    async def log(self, level: int, msg: str, **kwargs):
        boom()
