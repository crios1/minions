from typing import Any

def get_original_bases(cls: type) -> tuple[Any, ...]:
    try:
        from types import get_original_bases  # py3.12+
        return get_original_bases(cls)
    except Exception:
        return getattr(cls, "__orig_bases__", ())  # compat shim only