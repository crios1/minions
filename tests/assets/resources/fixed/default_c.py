from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    _value = 345

    async def get_value(self) -> int:
        return type(self)._value


resource = AssetResource
