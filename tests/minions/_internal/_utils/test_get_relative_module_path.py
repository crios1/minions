
import importlib
import sys
import types
from pathlib import Path
from uuid import uuid4

from minions._internal._utils.get_relative_module_path import get_relative_module_path

def test_returns_relative_path_when_module_file_under_cwd(tmp_path, monkeypatch):
    # Unique module name to avoid collisions across tests
    mod_name = f"mymod_{uuid4().hex}"

    # Arrange: create a real module under the working directory
    monkeypatch.chdir(tmp_path)
    (tmp_path / f"{mod_name}.py").write_text(
        "class C:\n"
        "    pass\n"
    )
    monkeypatch.syspath_prepend(str(tmp_path))

    try:
        mod = importlib.import_module(mod_name)
        cls = mod.C

        # Act
        result = get_relative_module_path(cls)

        # Assert: returns a Path relative to CWD
        assert result == Path(f"{mod_name}.py")
    finally:
        # Cleanup the imported temp module
        sys.modules.pop(mod_name, None)


def test_fallback_when_module_has_no_file(monkeypatch):
    # Unique module name
    fake_mod_name = f"fake_mod_no_file_{uuid4().hex}"
    fake_mod = types.ModuleType(fake_mod_name)
    monkeypatch.setitem(sys.modules, fake_mod_name, fake_mod)

    class C:
        pass

    # Assign the class to our fake module
    C.__module__ = fake_mod_name

    try:
        # Act
        result = get_relative_module_path(C)

        # Assert: falls back to "module.ClassName"
        assert result == f"{fake_mod_name}.C"
    finally:
        sys.modules.pop(fake_mod_name, None)


def test_fallback_when_module_path_not_under_cwd(tmp_path, monkeypatch):
    # Unique module name and separate dirs
    mod_name = f"mymod2_{uuid4().hex}"
    mod_dir = tmp_path / "moddir"
    cwd_dir = tmp_path / "othercwd"
    mod_dir.mkdir()
    cwd_dir.mkdir()

    (mod_dir / f"{mod_name}.py").write_text(
        "class C2:\n"
        "    pass\n"
    )

    monkeypatch.syspath_prepend(str(mod_dir))
    monkeypatch.chdir(cwd_dir)

    try:
        mod2 = importlib.import_module(mod_name)
        cls = mod2.C2

        # Act
        result = get_relative_module_path(cls)

        # Assert: relative_to() fails → fallback string
        assert result == f"{mod_name}.C2"
    finally:
        sys.modules.pop(mod_name, None)
