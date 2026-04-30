"""Shared serialization policy messages."""

from __future__ import annotations

SUPPORTED_TYPES_MSG = (
    "Supported types: "
    "(str, int, float, bool, None, bytes, "
    "list, tuple, dict[str, V], dataclass/msgspec.Struct/TypedDict)."
)
