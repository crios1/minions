from dataclasses import dataclass

import msgspec


@dataclass
class DataclassEvent:
    kind: str = "dataclass-event"
    payload_value: int = 10


@dataclass
class DataclassContext:
    seen_kind: str | None = None
    seen_value: int | None = None


class StructEvent(msgspec.Struct):
    kind: str = "struct-event"
    payload_value: int = 10


class StructContext(msgspec.Struct):
    seen_kind: str | None = None
    seen_value: int | None = None
