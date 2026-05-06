import asyncio
import sys

from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class BoomShutdownResource(SpiedResource):
    async def run(self) -> None:
        await asyncio.sleep(sys.maxsize)

    async def shutdown(self) -> None:
        boom()


resource = BoomShutdownResource
