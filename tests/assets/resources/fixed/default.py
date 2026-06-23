from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    _value = 123

    @classmethod
    def set_value(cls, value: int) -> None:
        cls._value = value

    async def startup(self):
        return

    async def shutdown(self):
        return

    async def run(self):
        return

    async def get_value(self) -> int:
        return type(self)._value


resource = AssetResource
