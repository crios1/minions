from tests.assets.resources.fixed.default import AssetResource as FixedResource
from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    fixed_resource: FixedResource
    startup_value: int | None = None

    async def startup(self):
        self.startup_value = await self.fixed_resource.get_value()

    async def get_value(self) -> int:
        return await self.fixed_resource.get_value()


resource = AssetResource
