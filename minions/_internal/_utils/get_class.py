import types
from typing import Any, Type, Union, get_origin, get_args

def get_class(hint: Any) -> Type | None:
    "Returns the first usable class from a type hint or None if no usable class is present."

    if get_origin(hint) in (Union, types.UnionType):
        for arg in get_args(hint):
            if arg is not type(None):
                return arg
        return None # pragma: no cover

    if hint is type(None):
        return None

    return hint
