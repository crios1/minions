"""Stamp durable UUID component ids into Minions source files."""

from __future__ import annotations

import argparse
import ast
import uuid
from dataclasses import dataclass
from pathlib import Path

__all__ = ["main"]


_DECORATOR_BY_KIND = {
    "minion": "minion_id",
    "pipeline": "pipeline_id",
    "resource": "resource_id",
}
_BASE_KIND_BY_NAME = {
    "Minion": "minion",
    "Pipeline": "pipeline",
    "Resource": "resource",
}
_SKIP_DIRS = {
    ".git",
    ".mypy_cache",
    ".pytest_cache",
    ".ruff_cache",
    ".tox",
    ".venv",
    "__pycache__",
    "build",
    "dist",
    "htmlcov",
    "venv",
}


@dataclass(frozen=True)
class Stamp:
    lineno: int
    decorator: str
    component_id: str
    class_name: str


@dataclass(frozen=True)
class StampResult:
    path: Path
    changed: bool
    stamps: tuple[Stamp, ...]


def _name_from_expr(node: ast.AST) -> str | None:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _name_from_expr(node.value)
    return None


def _decorator_name(node: ast.AST) -> str | None:
    if isinstance(node, ast.Call):
        return _name_from_expr(node.func)
    return _name_from_expr(node)


def _component_kind(node: ast.ClassDef) -> str | None:
    kinds = {
        _BASE_KIND_BY_NAME[base_name]
        for base in node.bases
        if (base_name := _name_from_expr(base)) in _BASE_KIND_BY_NAME
    }
    if len(kinds) == 1:
        return next(iter(kinds))
    return None


def _has_component_id_decorator(node: ast.ClassDef, kind: str) -> bool:
    expected = _DECORATOR_BY_KIND[kind]
    return any(_decorator_name(decorator) == expected for decorator in node.decorator_list)


def _stamps_for_source(source: str) -> tuple[Stamp, ...]:
    tree = ast.parse(source)
    stamps: list[Stamp] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        kind = _component_kind(node)
        if kind is None or _has_component_id_decorator(node, kind):
            continue
        insert_lineno = node.decorator_list[0].lineno if node.decorator_list else node.lineno
        stamps.append(
            Stamp(
                lineno=insert_lineno,
                decorator=_DECORATOR_BY_KIND[kind],
                component_id=str(uuid.uuid4()),
                class_name=node.name,
            )
        )
    return tuple(sorted(stamps, key=lambda item: item.lineno))


def _import_insert_index(lines: list[str]) -> int:
    index = 0
    if lines and lines[0].startswith("#!"):
        index = 1

    try:
        module = ast.parse("".join(lines))
    except SyntaxError:
        return index

    if (
        module.body
        and isinstance(module.body[0], ast.Expr)
        and isinstance(module.body[0].value, ast.Constant)
        and isinstance(module.body[0].value.value, str)
    ):
        index = module.body[0].end_lineno or module.body[0].lineno

    for node in module.body:
        if (
            isinstance(node, ast.ImportFrom)
            and node.module == "__future__"
            and node.end_lineno is not None
        ):
            index = max(index, node.end_lineno)
    return index


def _apply_stamps(source: str, stamps: tuple[Stamp, ...]) -> str:
    if not stamps:
        return source

    lines = source.splitlines(keepends=True)
    needs_import = sorted({stamp.decorator for stamp in stamps if stamp.decorator not in source})
    for stamp in sorted(stamps, key=lambda item: item.lineno, reverse=True):
        lines.insert(stamp.lineno - 1, f'@{stamp.decorator}("{stamp.component_id}")\n')

    if needs_import:
        import_line = f"from minions import {', '.join(needs_import)}\n"
        insert_index = _import_insert_index(lines)
        lines.insert(insert_index, import_line)
        if insert_index + 1 < len(lines) and lines[insert_index + 1].strip():
            lines.insert(insert_index + 1, "\n")

    return "".join(lines)


def stamp_file(path: Path, *, dry_run: bool = False) -> StampResult:
    source = path.read_text()
    stamps = _stamps_for_source(source)
    if not stamps:
        return StampResult(path=path, changed=False, stamps=())
    if not dry_run:
        path.write_text(_apply_stamps(source, stamps))
    return StampResult(path=path, changed=True, stamps=stamps)


def _iter_python_files(root: Path) -> list[Path]:
    if root.is_file():
        if root.suffix != ".py":
            raise ValueError(f"Unsupported component source file for {root}: expected .py")
        return [root]
    if not root.is_dir():
        raise FileNotFoundError(root)

    files: list[Path] = []
    for path in root.rglob("*.py"):
        if any(
            part.startswith(".") or part in _SKIP_DIRS for part in path.relative_to(root).parts[:-1]
        ):
            continue
        files.append(path)
    return sorted(files)


def expand_paths(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        files.extend(_iter_python_files(path))
    return files


def build_parser(*, prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Stamp durable UUID component ids on Minion, Pipeline, and Resource classes.",
    )
    parser.add_argument(
        "paths", nargs="+", type=Path, help="Python source files or directories to update."
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Show what would be stamped without writing files."
    )
    return parser


def main(
    argv: list[str] | None = None, *, prog: str = "python -m minions stamp component-ids"
) -> int:
    parser = build_parser(prog=prog)
    args = parser.parse_args(argv)
    for path in expand_paths(args.paths):
        result = stamp_file(path, dry_run=args.dry_run)
        if not result.stamps:
            print(f"{path}: no missing component ids")
            continue
        action = "would stamp" if args.dry_run else "stamped"
        for stamp in result.stamps:
            print(f"{path}:{stamp.lineno}: {action} {stamp.class_name} with @{stamp.decorator}")
    return 0
