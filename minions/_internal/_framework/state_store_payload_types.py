from __future__ import annotations

from typing import TypeAlias


SerializableValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | bytes
    | None
    | list["SerializableValue"]
    | tuple["SerializableValue", ...]
    | dict[str | int, "SerializableValue"]
)

StateStorePayload: TypeAlias = dict[str, SerializableValue]
