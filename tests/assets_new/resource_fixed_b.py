from tests.assets.support.resource_spied import SpiedResource


class FixedResourceB(SpiedResource):
    name = "fixed-resource-b"
    _value = 234

    @classmethod
    def set_value(cls, value: int) -> None:
        cls._value = value

    async def startup(self):
        return

    async def shutdown(self):
        return

    async def run(self):
        return

    async def get_value(self, _hint: int | None = None) -> int:
        return type(self)._value


resource = FixedResourceB
