from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    _value = 1

    async def startup(self):
        return

    async def shutdown(self):
        return

    async def run(self):
        return

    @SpiedResource.untracked
    async def _method1(self):
        return

    @SpiedResource.untracked()
    async def _method2(self):
        return

    @SpiedResource.untracked(kwarg="kwargs")
    async def _method3(self):
        return

    async def get_value(self) -> int:
        await self._method1()
        await self._method2()
        await self._method3()
        return type(self)._value


resource = AssetResource
