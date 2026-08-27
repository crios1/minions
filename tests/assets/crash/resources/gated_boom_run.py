import asyncio

from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    async def startup(self) -> None:
        self._release_run_failure = asyncio.Event()

    def trigger_run_failure(self) -> None:
        self._release_run_failure.set()

    async def run(self) -> None:
        await self._release_run_failure.wait()
        boom()


resource = AssetResource
