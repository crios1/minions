from tests.assets.support.resource_spied import SpiedResource


class AssetResource(SpiedResource):
    _value = 345

    @classmethod
    def set_value(cls, value: int) -> None:
        cls._value = value

    async def get_value(self) -> int:
        return type(self)._value


resource = AssetResource
