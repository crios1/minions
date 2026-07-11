import msgspec


class IntValueContext(msgspec.Struct):
    value: int = 0
