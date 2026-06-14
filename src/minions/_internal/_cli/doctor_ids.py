"""Inspect durable Minions identity metadata."""

from __future__ import annotations

import argparse
import ast
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from minions._internal._cli.component_id_source import (
    DECORATOR_BY_KIND,
    component_id_for_class,
    component_kind,
    module_string_constants,
)
from minions._internal._cli.component_id_stamp import (
    expand_paths as expand_component_paths,
)
from minions._internal._cli.config_id_stamp import expand_paths as expand_config_paths
from minions._internal._domain.component_identity import validate_component_id
from minions._internal._domain.config_identity import get_config_id

__all__ = ["main"]

_CONFIG_SUFFIXES = {".toml", ".yaml", ".yml", ".json"}


@dataclass(frozen=True)
class Issue:
    path: Path
    lineno: int | None
    code: str
    message: str


@dataclass(frozen=True)
class ComponentIdentity:
    kind: str
    component_id: str
    path: Path
    lineno: int
    class_name: str


@dataclass(frozen=True)
class ConfigIdentity:
    config_id: str
    path: Path


# Note: keep this path classification in sync with stamp_all._split_paths.
# If either command gains new path semantics, consider extracting a shared helper.
def _split_paths(paths: list[Path]) -> tuple[list[Path], list[Path]]:
    component_paths: list[Path] = []
    config_paths: list[Path] = []
    for path in paths:
        if path.is_dir():
            component_paths.append(path)
            config_paths.append(path)
        elif path.suffix == ".py":
            component_paths.append(path)
        elif path.suffix.lower() in _CONFIG_SUFFIXES:
            config_paths.append(path)
        else:
            raise ValueError(
                (
                    f"Unsupported doctor ids path for {path}: "
                    "expected a directory, .py, .toml, .yaml, .yml, or .json"
                )
            )
    return component_paths, config_paths


def inspect_component_file(path: Path) -> tuple[list[ComponentIdentity], list[Issue]]:
    source = path.read_text()
    tree = ast.parse(source)
    constants = module_string_constants(tree)
    identities: list[ComponentIdentity] = []
    issues: list[Issue] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        kind = component_kind(node)
        if kind is None:
            continue
        component_id, decorator_lineno = component_id_for_class(node, kind, constants)
        if component_id is None:
            issues.append(
                Issue(
                    path=path,
                    lineno=node.lineno,
                    code="missing-component-id",
                    message=f"{node.name} is missing @{DECORATOR_BY_KIND[kind]}",
                )
            )
            continue
        try:
            component_id = validate_component_id(component_id)
        except Exception as e:
            issues.append(
                Issue(
                    path=path,
                    lineno=decorator_lineno,
                    code="invalid-component-id",
                    message=f"{node.name} has invalid component id: {e}",
                )
            )
            continue
        identities.append(
            ComponentIdentity(
                kind=kind,
                component_id=component_id,
                path=path,
                lineno=decorator_lineno or node.lineno,
                class_name=node.name,
            )
        )
    return identities, issues


def inspect_config_file(path: Path) -> tuple[ConfigIdentity | None, list[Issue]]:
    try:
        config_id = get_config_id(str(path))
    except Exception as e:
        return None, [
            Issue(
                path=path,
                lineno=None,
                code="invalid-config-id",
                message=f"invalid _minions_config_id: {e}",
            )
        ]
    if config_id is None:
        return None, [
            Issue(
                path=path,
                lineno=None,
                code="missing-config-id",
                message="missing _minions_config_id",
            )
        ]
    return ConfigIdentity(config_id=config_id, path=path), []


def _duplicate_component_issues(identities: list[ComponentIdentity]) -> list[Issue]:
    by_key: dict[tuple[str, str], list[ComponentIdentity]] = defaultdict(list)
    for identity in identities:
        by_key[(identity.kind, identity.component_id)].append(identity)
    issues: list[Issue] = []
    for (kind, component_id), matches in by_key.items():
        if len(matches) < 2:
            continue
        locations = ", ".join(f"{item.path}:{item.lineno}" for item in matches)
        for item in matches:
            issues.append(
                Issue(
                    path=item.path,
                    lineno=item.lineno,
                    code="duplicate-component-id",
                    message=f"duplicate {kind} component id {component_id}: {locations}",
                )
            )
    return issues


def _duplicate_config_issues(identities: list[ConfigIdentity]) -> list[Issue]:
    by_id: dict[str, list[ConfigIdentity]] = defaultdict(list)
    for identity in identities:
        by_id[identity.config_id].append(identity)
    issues: list[Issue] = []
    for config_id, matches in by_id.items():
        if len(matches) < 2:
            continue
        locations = ", ".join(str(item.path) for item in matches)
        for item in matches:
            issues.append(
                Issue(
                    path=item.path,
                    lineno=None,
                    code="duplicate-config-id",
                    message=f"duplicate config id {config_id}: {locations}",
                )
            )
    return issues


def inspect_paths(paths: list[Path]) -> list[Issue]:
    component_roots, config_roots = _split_paths(paths)
    component_identities: list[ComponentIdentity] = []
    config_identities: list[ConfigIdentity] = []
    issues: list[Issue] = []

    for path in expand_component_paths(component_roots):
        identities, path_issues = inspect_component_file(path)
        component_identities.extend(identities)
        issues.extend(path_issues)

    for path in expand_config_paths(config_roots):
        identity, path_issues = inspect_config_file(path)
        if identity is not None:
            config_identities.append(identity)
        issues.extend(path_issues)

    issues.extend(_duplicate_component_issues(component_identities))
    issues.extend(_duplicate_config_issues(config_identities))
    return sorted(issues, key=lambda item: (str(item.path), item.lineno or 0, item.code))


def build_parser(*, prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Inspect durable component and config identity metadata.",
    )
    parser.add_argument("paths", nargs="+", type=Path, help="Files or directories to inspect.")
    return parser


def main(argv: list[str] | None = None, *, prog: str = "python -m minions doctor ids") -> int:
    parser = build_parser(prog=prog)
    args = parser.parse_args(argv)
    issues = inspect_paths(args.paths)
    if not issues:
        print("No identity issues found.")
        return 0

    for issue in issues:
        location = f"{issue.path}:{issue.lineno}" if issue.lineno is not None else str(issue.path)
        print(f"{location}: {issue.code}: {issue.message}")
    return 1
