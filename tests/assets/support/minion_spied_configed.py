import asyncio
import tomllib
from dataclasses import dataclass
from pathlib import Path
from typing import cast

from minions._internal._domain.types import T_Ctx, T_Event

from .minion_spied import SpiedMinion


@dataclass
class AssetMinionConfig:
    name: str


class ConfiguredSpiedMinion(SpiedMinion[T_Event, T_Ctx], defer_minion_setup=True):
    config: AssetMinionConfig

    async def load_config(self, config_path: str) -> AssetMinionConfig:
        contents = await asyncio.to_thread(Path(config_path).read_bytes)
        parsed = tomllib.loads(contents.decode())
        config = parsed.get("config")

        if not isinstance(config, dict):
            raise TypeError("Asset minion config requires [config]")

        config = cast(dict[str, object], config)
        name = config.get("name")

        if not isinstance(name, str):
            raise TypeError("Asset minion config requires config.name")

        return AssetMinionConfig(name=name)
