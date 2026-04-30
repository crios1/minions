"""Serialization-specific static type checks."""

from __future__ import annotations

import typing
from collections.abc import Mapping
from dataclasses import fields, is_dataclass
from typing import Any, get_args, get_origin, get_type_hints

import msgspec

from .constants import SUPPORTED_TYPES_MSG


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


def _is_msgspec_struct_type(tp: Any) -> bool:
    try:
        return isinstance(tp, type) and issubclass(tp, msgspec.Struct)
    except TypeError:  # pragma: no cover
        return False


def _normalize_origin_args(tp: Any) -> tuple[Any, tuple[Any, ...]]:
    """Return `(get_origin(tp) or tp, get_args(tp))`."""
    origin = get_origin(tp) or tp
    args = get_args(tp)
    return origin, args

def _mapping_value_type_if_str_key(origin: Any, args: tuple[Any, ...]) -> tuple[bool, Any]:
    """Return `(ok, value_type)` for dict-like annotations using `str` keys."""
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

def _is_serializable_leaf_type(tp: Any) -> bool:
    return tp in (str, int, float, bool, type(None), bytes)

def _is_serializable_field_type(tp: Any) -> bool:
    """Return whether a field type is representable by msgspec msgpack."""
    stack = [tp]

    while stack:
        t = stack.pop()

        if _is_serializable_leaf_type(t):
            continue

        origin, args = _normalize_origin_args(t)

        if origin is typing.Union:
            stack.extend(args)
            continue

        if origin is list or t is list:
            if not args:
                continue
            if len(args) != 1:
                return False
            stack.append(args[0])
            continue

        if origin is tuple or t is tuple:
            if not args:
                continue
            if len(args) == 2 and args[1] is Ellipsis:
                stack.append(args[0])
                continue
            stack.extend(args)
            continue

        if origin is dict or t is dict:
            if not args:
                continue
            if len(args) != 2:
                return False
            k, v = args
            if k is not str:
                return False
            stack.append(v)
            continue

        if _is_dataclass_type(t):
            hints = get_type_hints(t, include_extras=True)
            stack.extend(hints[f.name] for f in fields(t))
            continue

        if _is_msgspec_struct_type(t):
            hints = get_type_hints(t, include_extras=True)
            stack.extend(hints[f.name] for f in msgspec.structs.fields(t))
            continue

        if _is_typed_dict_type(t):
            hints = get_type_hints(t, include_extras=True)
            stack.extend(hints.values())
            continue

        try:
            if _is_mapping_type(t):
                origin, args = _normalize_origin_args(t)
                ok, v = _mapping_value_type_if_str_key(origin, args)
                if not ok:
                    return False
                if v is not None:
                    stack.append(v)
                    continue
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

    Caveats: this is a static/type-level check only - it does not instantiate
    classes or validate dataclass default-factory return values. Runtime
    defaults may still be non-serializable even when annotations look valid.
    """
    return _is_serializable_field_type(tp)


def require_type_serializable(tp: Any, *, owner: str, type_label: str) -> None:
    """Raise TypeError when `tp` is not accepted by `is_type_serializable`."""
    if is_type_serializable(tp):
        return

    raise TypeError(
        f"{owner}: {type_label} is not serializable. "
        f"{SUPPORTED_TYPES_MSG}"
    )
