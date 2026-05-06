import asyncio
import sys

from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class BoomMethodResource(SpiedResource):
    async def run(self) -> None:
        await asyncio.sleep(sys.maxsize)

    async def explode(self) -> None:
        boom()


resource = BoomMethodResource
