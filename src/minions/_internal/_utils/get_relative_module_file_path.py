import sys
from pathlib import Path


def get_relative_module_file_path(cls: type, *, relative_to: Path) -> Path:
    """Return the class module's file path relative to an explicit base directory."""
    try:
        module = sys.modules[cls.__module__]
    except KeyError as error:
        raise LookupError(f"Module {cls.__module__!r} is not loaded.") from error

    module_file = getattr(module, "__file__", None)
    if module_file is None:
        raise ValueError(f"Module {cls.__module__!r} has no file path.")

    return Path(module_file).resolve().relative_to(relative_to.resolve())
