import asyncio

from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics
from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    def __init__(
        self,
        logger: Logger,
        metrics: Metrics,
        resource_module_path: str,
        resource_id: str,
    ) -> None:
        super().__init__(logger, metrics, resource_module_path, resource_id)
        self._release_run_failure = asyncio.Event()

    def trigger_run_failure(self) -> None:
        self._release_run_failure.set()

    async def run(self) -> None:
        await self._release_run_failure.wait()
        boom()


resource = AssetResource
