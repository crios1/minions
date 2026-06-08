import sys
from pathlib import Path


def get_relative_module_path(cls: type) -> Path | str:
    "Gets relative module path to current working directory, falling back to module.ClassName."
    fallback = f"{cls.__module__}.{cls.__name__}"
    try:
        mod = sys.modules[cls.__module__]
        mod_file = getattr(mod, "__file__", None)
        if mod_file is None:
            return fallback
        return Path(mod_file).resolve().relative_to(Path.cwd())
    except (KeyError, ValueError):
        return fallback
