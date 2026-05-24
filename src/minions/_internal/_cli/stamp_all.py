"""Stamp all durable Minions IDs under files or directories."""

from __future__ import annotations

import argparse
from pathlib import Path

from minions._internal._cli.component_id_stamp import (
    expand_paths as expand_component_paths,
)
from minions._internal._cli.component_id_stamp import (
    stamp_file as stamp_component_file,
)
from minions._internal._cli.config_id_stamp import (
    expand_paths as expand_config_paths,
)
from minions._internal._cli.config_id_stamp import (
    stamp_file as stamp_config_file,
)
from minions._internal._domain.config_identity import CONFIG_ID_KEY

__all__ = ["main"]

_CONFIG_SUFFIXES = {".toml", ".yaml", ".yml", ".json"}


# Note: keep this path classification in sync with doctor_ids._split_paths.
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
                f"Unsupported stamp all path for {path}: expected a directory, .py, .toml, .yaml, .yml, or .json"
            )
    return component_paths, config_paths


def build_parser(*, prog: str) -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog=prog,
        description="Stamp durable component and config UUIDs under files or directories.",
    )
    parser.add_argument("paths", nargs="+", type=Path, help="Files or directories to update.")
    parser.add_argument("--dry-run", action="store_true", help="Show what would be stamped without writing files.")
    return parser


def main(argv: list[str] | None = None, *, prog: str = "python -m minions stamp all") -> int:
    parser = build_parser(prog=prog)
    args = parser.parse_args(argv)
    component_roots, config_roots = _split_paths(args.paths)

    for path in expand_component_paths(component_roots):
        result = stamp_component_file(path, dry_run=args.dry_run)
        if not result.stamps:
            print(f"{path}: no missing component ids")
            continue
        action = "would stamp" if args.dry_run else "stamped"
        for stamp in result.stamps:
            print(f"{path}:{stamp.lineno}: {action} {stamp.class_name} with @{stamp.decorator}")

    for path in expand_config_paths(config_roots):
        changed = stamp_config_file(path, dry_run=args.dry_run)
        if changed:
            action = "would stamp" if args.dry_run else "stamped"
            print(f"{path}: {action} {CONFIG_ID_KEY}")
        else:
            print(f"{path}: already has {CONFIG_ID_KEY}")

    return 0
