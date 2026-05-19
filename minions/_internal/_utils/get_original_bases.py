import types
from typing import Any


def get_original_bases(cls: type) -> tuple[Any, ...]:
    # Python 3.12+
    get_original_bases = getattr(types, "get_original_bases", None)

    # Python < 3.12
    if get_original_bases is None:
        return getattr(cls, "__orig_bases__", ())

    return get_original_bases(cls)
