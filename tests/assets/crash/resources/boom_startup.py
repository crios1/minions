from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class BoomStartupResource(SpiedResource):
    async def startup(self) -> None:
        boom()

    async def run(self) -> None:
        return


resource = BoomStartupResource
