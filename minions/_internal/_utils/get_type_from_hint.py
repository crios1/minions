import types
from typing import Any, Union, get_args, get_origin


def get_type_from_hint(hint: Any) -> type[Any] | None:
    "Returns the first usable runtime type from a type hint or None."

    origin = get_origin(hint)

    if origin in (Union, types.UnionType):
        for arg in get_args(hint):
            typ = get_type_from_hint(arg)

            if typ is not None:
                return typ

        return None  # pragma: no cover

    if hint is type(None):
        return None

    if hint is Any:
        return None

    if not isinstance(hint, type):
        return None

    return hint
