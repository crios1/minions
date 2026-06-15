import importlib
import sys
import types
from pathlib import Path
from textwrap import dedent
from uuid import uuid4

import pytest

from minions._internal._utils.get_relative_module_file_path import (
    get_relative_module_file_path,
)


def test_returns_module_file_path_relative_to_explicit_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod_name = f"mymod_{uuid4().hex}"
    (tmp_path / f"{mod_name}.py").write_text(
        dedent(
            """\
            class C:
                pass
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))  # pyright: ignore[reportUnknownMemberType]

    try:
        module = importlib.import_module(mod_name)

        result = get_relative_module_file_path(module.C, relative_to=tmp_path)

        assert result == Path(f"{mod_name}.py")
    finally:
        sys.modules.pop(mod_name, None)


def test_returns_nested_module_file_path_relative_to_explicit_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    package_name = f"package_{uuid4().hex}"
    package_dir = tmp_path / package_name
    nested_dir = package_dir / "nested"
    nested_dir.mkdir(parents=True)
    (package_dir / "__init__.py").write_text("")
    (nested_dir / "__init__.py").write_text("")
    (nested_dir / "component.py").write_text(
        dedent(
            """\
            class C:
                pass
            """
        )
    )
    monkeypatch.syspath_prepend(str(tmp_path))  # pyright: ignore[reportUnknownMemberType]
    module_name = f"{package_name}.nested.component"

    try:
        module = importlib.import_module(module_name)

        result = get_relative_module_file_path(module.C, relative_to=tmp_path)

        assert result == Path(package_name) / "nested" / "component.py"
    finally:
        for loaded_module_name in (
            module_name,
            f"{package_name}.nested",
            package_name,
        ):
            sys.modules.pop(loaded_module_name, None)


def test_raises_when_module_is_not_loaded() -> None:
    class C:
        pass

    C.__module__ = f"missing_module_{uuid4().hex}"

    with pytest.raises(LookupError, match="is not loaded"):
        get_relative_module_file_path(C, relative_to=Path.cwd())


def test_raises_when_module_has_no_file(monkeypatch: pytest.MonkeyPatch) -> None:
    mod_name = f"module_without_file_{uuid4().hex}"
    module = types.ModuleType(mod_name)
    monkeypatch.setitem(sys.modules, mod_name, module)

    class C:
        pass

    C.__module__ = mod_name

    with pytest.raises(ValueError, match="has no file path"):
        get_relative_module_file_path(C, relative_to=Path.cwd())


def test_raises_when_module_file_is_outside_explicit_base(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mod_name = f"mymod_{uuid4().hex}"
    module_dir = tmp_path / "module"
    base_dir = tmp_path / "base"
    module_dir.mkdir()
    base_dir.mkdir()
    (module_dir / f"{mod_name}.py").write_text(
        dedent(
            """\
            class C:
                pass
            """
        )
    )
    monkeypatch.syspath_prepend(str(module_dir))  # pyright: ignore[reportUnknownMemberType]

    try:
        module = importlib.import_module(mod_name)

        with pytest.raises(ValueError):
            get_relative_module_file_path(module.C, relative_to=base_dir)
    finally:
        sys.modules.pop(mod_name, None)
