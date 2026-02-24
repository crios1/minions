"""msgspec msgpack encoding/decoding primitives with stable error mapping."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, TypeVar, overload

import msgspec

T = TypeVar("T")

_ENCODER = msgspec.msgpack.Encoder()

SUPPORTED_TYPES_MSG = (
    "Supported types: "
    "(str, int, float, bool, None, bytes, "
    "list, tuple, dict[str, V], dataclass/msgspec.Struct/TypedDict)."
)


def serialize(obj: Any, *, exp_msg_prefix: str = "") -> bytes:
    """Encode using msgspec MsgPack with consistent error mapping."""
    try:
        return _ENCODER.encode(obj)
    except (msgspec.EncodeError, TypeError) as e:
        name = getattr(obj, "__name__", obj.__class__.__name__)
        raise TypeError(
            f"{exp_msg_prefix}{name} is not serializable. {SUPPORTED_TYPES_MSG} ({e})"
        ) from e


@lru_cache(maxsize=64)
def _cached_decoder(t: Any) -> msgspec.msgpack.Decoder:
    """Hashable types/annotations only."""
    return msgspec.msgpack.Decoder(type=t)

def _get_decoder(t: Any) -> msgspec.msgpack.Decoder:
    """Return a decoder for `t`, caching only when `t` is hashable."""
    try:
        return _cached_decoder(t)
    except TypeError:
        # Unhashable key for lru_cache; skip caching
        return msgspec.msgpack.Decoder(type=t)

@overload
def deserialize(payload: bytes, type_: type[T], *, exp_msg_prefix: str = "") -> T: ...

@overload
def deserialize(payload: bytes, type_: Any, *, exp_msg_prefix: str = "") -> Any: ...

def deserialize(
    payload: bytes,
    type_: Any,
    *,
    exp_msg_prefix: str = "",
) -> Any:
    if type_ is None:
        raise TypeError(f"{exp_msg_prefix}type_ is required for typed decoding")
    try:
        return _get_decoder(type_).decode(payload)
    except (msgspec.DecodeError, msgspec.ValidationError) as e:
        name = getattr(type_, "__qualname__", getattr(type_, "__name__", repr(type_)))
        raise ValueError(f"{exp_msg_prefix}invalid payload for {name}: {e}") from e
