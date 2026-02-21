from tests.assets.support.resource_spied import SpiedResource


class SimpleResource3(SpiedResource):
    name = "simple-resource-3"

    async def startup(self):
        self.api = {"/price?symbol=TSLA": 265.0}

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

    async def get_price(self, timestamp: float | None = None):
        await self._method1()
        await self._method2()
        await self._method3()
        return self.api.get("/price?symbol=NVDA", None)


resource = SimpleResource3
