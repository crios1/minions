from tests.assets.support.resource_spied import SpiedResource


class FixedResource(SpiedResource):
    name = "fixed-resource"
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

    async def get_value(self, _hint: int | None = None) -> int:
        return type(self)._value


resource = FixedResource
