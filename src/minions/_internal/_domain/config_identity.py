"""Minion config identity helpers used by Gru orchestration identity.

This module is separate from gru.py so config ID parsing and validation can be
shared by Gru and the CLI stamping/diagnostic tools. Conceptually, the identity
still belongs to Gru's orchestration boundary: Gru uses it to derive stable
orchestration IDs for file-backed minion configs, while the CLI exists to help
users maintain that runtime contract.

If Gru is later split into a package/module hierarchy, this module should likely
move under that identity boundary, with the CLI continuing to import it from
there.
"""

from __future__ import annotations

import json
import tomllib
import uuid
from pathlib import Path
from typing import Any, cast

CONFIG_ID_KEY = "_minions_config_id"
_YAML_SUFFIXES = {".yaml", ".yml"}


def validate_config_id(config_id: object) -> str:
    if not isinstance(config_id, str):
        raise TypeError("config id must be a string")
    config_id = config_id.strip()
    if not config_id:
        raise ValueError("config id must not be empty")
    try:
        parsed = uuid.UUID(config_id)
    except ValueError as e:
        raise ValueError("config id must be a canonical UUID string") from e
    if str(parsed) != config_id:
        raise ValueError("config id must be a canonical lowercase UUID string")
    return config_id


def _read_supported_config(path: Path) -> dict[Any, Any] | None:
    suffix = path.suffix.lower()
    if suffix == ".toml":
        parsed = tomllib.loads(path.read_text())
    elif suffix == ".json":
        parsed = json.loads(path.read_text())
    elif suffix in _YAML_SUFFIXES:
        # manual read to avoid yml parser dependency
        value = None
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if not line.startswith(f"{CONFIG_ID_KEY}:"):
                continue
            raw_value = line.removeprefix(f"{CONFIG_ID_KEY}:").strip()
            if not raw_value:
                value = ""
            elif raw_value[0] in ('"', "'") and raw_value[-1:] == raw_value[0]:
                value = raw_value[1:-1]
            else:
                value = raw_value.split(" #", 1)[0].strip()
            break
        return {} if value is None else {CONFIG_ID_KEY: value}
    else:
        return None
    if not isinstance(parsed, dict):
        return None
    parsed = cast(dict[Any, Any], parsed)
    return parsed


def get_config_id(config_path: str) -> str | None:
    parsed = _read_supported_config(Path(config_path))
    if parsed is None or CONFIG_ID_KEY not in parsed:
        return None
    value = parsed[CONFIG_ID_KEY]
    if not isinstance(value, str):
        raise TypeError(f"{CONFIG_ID_KEY} must be a string")
    return validate_config_id(value)
