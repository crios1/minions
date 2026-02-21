from tests.assets.support.resource_spied import SpiedResource


class ErrorResource(SpiedResource):
    name = "error-resource"

    async def startup(self):
        return

    async def shutdown(self):
        return

    async def run(self):
        return

    async def explode(self):
        raise RuntimeError("intentional resource error")


resource = ErrorResource
