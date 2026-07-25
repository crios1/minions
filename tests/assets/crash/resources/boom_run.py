import asyncio

from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    fail_run = asyncio.Event()

    async def run(self) -> None:
        await type(self).fail_run.wait()
        boom()


resource = AssetResource
