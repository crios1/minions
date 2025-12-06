import sys
from pathlib import Path

def get_relative_module_path(cls: type):
    "gets relative module path to current working directory, with fallback"
    try:
        mod = sys.modules[cls.__module__]
        return Path(mod.__file__).resolve().relative_to(Path.cwd())  # type: ignore
    except Exception:
        return f"{cls.__module__}.{cls.__name__}"
