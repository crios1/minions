"""Serialization primitives using msgspec for fast, explicit, schema-driven encoding with minimal runtime overhead."""

from __future__ import annotations

from functools import lru_cache
from typing import Any, TypeVar, overload

import msgspec

T = TypeVar("T")

_ENCODER = msgspec.msgpack.Encoder()

# TODO: i need to clean up this file and probably it's tests too (true, the need to be consolidated)
# TODO: i need to write test(s) to determine this msg is correct
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

# TODO: technically, i should remove lru_cache and just use a dict
#       (add/remove decoders from {} based on on the pipelines and minions running)
#       (store decoders for event and workflowcontext types)
#       (that way i store just what i need at all points in time, and ensures clean up too)
#       but i'll do that some other time after the mvp
@lru_cache(maxsize=64)
def _cached_decoder(t: Any) -> msgspec.msgpack.Decoder:
    """Hashable types/annotations only."""
    return msgspec.msgpack.Decoder(type=t)

def _get_decoder(t: Any) -> msgspec.msgpack.Decoder:
    """
    Return a decoder for 't', using the cache when possible.
    Falls back to a one-off decoder if 't' isn't hashable (older typing objects, etc.).
    """
    try:
        return _cached_decoder(t)
    except TypeError:
        # Unhashable key for lru_cache; don't cache this one.
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
        type_label = getattr(type_, "__qualname__", getattr(type_, "__name__", repr(type_)))
        raise ValueError(f"{exp_msg_prefix}invalid payload for {type_label}: {e}") from e

###############

from collections.abc import Mapping
from dataclasses import is_dataclass, fields
from typing import Any, get_origin, get_args, get_type_hints
import typing


def _normalize_origin_args(tp: Any) -> tuple[Any, tuple[Any, ...]]:
    """Return a normalized (origin, args) pair for a typing object.

    `origin` will be the value of `get_origin(tp)` or `tp` when no origin
    exists. `args` is the tuple returned from `get_args(tp)`.
    """
    origin = get_origin(tp) or tp
    args = get_args(tp)
    return origin, args


def _mapping_value_type_if_str_key(origin: Any, args: tuple[Any, ...]) -> tuple[bool, Any]:
    """Helper for dict-like types.

    Returns (ok, value_type) where `ok` is False when the mapping's key
    type is not `str` (i.e., not allowed), True when acceptable (including
    len(args) != 2 case which is treated as a best-effort True), and
    `value_type` is the value type when available or None.
    """
    if origin is dict:
        if not args:
            return True, None
        if len(args) != 2:
            return True, None
        k, v = args
        if k is not str:
            return False, None
        return True, v

    # Non-dict mapping subclasses: best-effort accept without a value type
    return True, None

def _is_typed_dict_type(tp: Any) -> bool:
    return (
        isinstance(tp, type)
        and hasattr(tp, "__required_keys__")
        and hasattr(tp, "__optional_keys__")
    )

def _is_mapping_type(tp: Any) -> bool:
    origin = get_origin(tp) or tp
    try:
        return origin is dict or issubclass(origin, Mapping)
    except TypeError:  # pragma: no cover
        return False

def _is_dataclass_type(tp: Any) -> bool:
    return isinstance(tp, type) and is_dataclass(tp)

def _is_json_leaf_type(tp: Any) -> bool:
    return tp in (str, int, float, bool, type(None), bytes)

def _is_json_jsonable_field_type(tp: Any) -> bool:
    """
    Determine whether a field type is representable by msgspec/msgpack.
    Accepts primitives, bytes, lists/tuples/dicts (parameterized or bare),
    dataclasses, TypedDicts, and unions where every option is serializable
    (treats Optional[T] as Union[T, None]).
    """
    stack = [tp]

    while stack:
        t = stack.pop()

        if _is_json_leaf_type(t):
            continue

        origin, args = _normalize_origin_args(t)

        # Handle typing.Union / Optional
        if origin is typing.Union:
            # all args must be serializable
            for a in args:
                stack.append(a)
            continue

        # lists
        if origin is list or t is list:
            if not args:
                continue  # bare list allowed
            if len(args) != 1:
                return False
            stack.append(args[0])
            continue

        # tuples
        if origin is tuple or t is tuple:
            if not args:
                continue
            if len(args) == 2 and args[1] is Ellipsis:
                stack.append(args[0])
                continue
            stack.extend(a for a in args)
            continue

        # dicts and dict-like mappings
        if origin is dict or t is dict:
            if not args:
                continue  # bare dict allowed
            if len(args) != 2:
                return False
            k, v = args
            if k is not str:
                return False
            stack.append(v)
            continue

        # dataclasses
        if _is_dataclass_type(t):
            hints = get_type_hints(t, include_extras=True)
            stack.extend(hints[f.name] for f in fields(t))
            continue

        # TypedDict
        if _is_typed_dict_type(t):
            hints = get_type_hints(t, include_extras=True)
            stack.extend(hints.values())
            continue

        # Mapping subclasses with params
        try:
            if _is_mapping_type(t):
                origin, args = _normalize_origin_args(t)
                ok, v = _mapping_value_type_if_str_key(origin, args)
                if not ok:
                    return False
                if v is not None:
                    stack.append(v)
                    continue
                # otherwise best-effort continue
                continue
        except Exception:
            pass

        return False

    return True

def is_type_serializable(tp: Any) -> bool:
    """Return True when a type is likely serializable by msgspec/msgpack.

    Policy: accepts primitive leaf types (str,int,float,bool,None,bytes),
    bare or parameterized containers (list/tuple/dict where dict keys are
    `str`), dataclasses, TypedDicts and Mapping[...] when their value types
    are serializable. Top-level Union/Optional is allowed only if every
    alternative is serializable.

    Caveats: this is a static/type-level check only — it does not instantiate
    classes or validate dataclass default-factory return values. Runtime
    defaults may still be non-serializable even when annotations look valid.
    """

    # Handle top-level Union/Optional
    origin = get_origin(tp)
    if origin is typing.Union:
        return all(is_type_serializable(a) for a in get_args(tp))

    # Primitive / leaf types and bytes
    if _is_json_leaf_type(tp):
        return True

    if tp is bytes:
        return True

    # Bare container types
    origin = get_origin(tp) or tp

    if origin is list or tp is list:
        args = get_args(tp)
        if not args:
            return True
        return _is_json_jsonable_field_type(tp)

    if origin is tuple or tp is tuple:
        args = get_args(tp)
        if not args:
            return True
        return _is_json_jsonable_field_type(tp)

    if origin is dict or tp is dict:
        args = get_args(tp)
        if not args:
            return True
        k, v = args
        if k is not str:
            return False
        return _is_json_jsonable_field_type(v)

    if _is_dataclass_type(tp):
        hints = get_type_hints(tp, include_extras=True)
        return all(_is_json_jsonable_field_type(hints[f.name]) for f in fields(tp))

    if _is_typed_dict_type(tp):
        hints = get_type_hints(tp, include_extras=True)
        return all(_is_json_jsonable_field_type(t) for t in hints.values())

    if _is_mapping_type(tp):
        origin = get_origin(tp)
        if origin is dict:
            args = get_args(tp)
            if len(args) != 2:
                return True
            k, v = args
            if k is not str:
                return False
            return _is_json_jsonable_field_type(v)
        return True

    return False
