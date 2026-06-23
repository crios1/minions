from minions import resource_id
from tests.assets.support.resource_spied import SpiedResource


@resource_id("22345678-1234-5678-9234-567812345678")
class AssetResource(SpiedResource):
    _value = 123

    async def get_value(self, _hint: int | None = None) -> int:
        return type(self)._value


resource = AssetResource
