"""Shared serialization policy messages."""

from __future__ import annotations

SERIALIZABLE_PRIMITIVE_TYPES = (str, int, float, bool, type(None), bytes)

SUPPORTED_TYPES_MSG = (
    "Supported types: "
    "(str, int, float, bool, None, bytes, "
    "list, tuple, dict[str, V], dataclass/msgspec.Struct/TypedDict)."
)

SUPPORTED_USER_DECLARED_TYPES_MSG = (
    "Supported user-declared types: "
    "(dataclass, msgspec.Struct)."
)
