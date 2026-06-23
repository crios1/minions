from tests.assets.crash.boom import boom
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    async def startup(self) -> None:
        boom()

    async def run(self) -> None:
        return


resource = AssetResource
