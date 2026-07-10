import msgspec


class IntValueEvent(msgspec.Struct):
    value: int = 0
