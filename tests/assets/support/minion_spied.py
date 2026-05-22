import asyncio
import importlib
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, cast

from minions._internal._domain.minion import Minion
from .mixin_spy import SpyMixin

from minions._internal._domain.types import T_Event, T_Ctx


@dataclass
class AssetMinionConfig:
    name: str


async def load_asset_minion_config(config_path: str) -> AssetMinionConfig:
    contents = await asyncio.to_thread(Path(config_path).read_bytes)
    toml_module = importlib.import_module("tomllib")
    loads = cast(Callable[[str], dict[str, object]], getattr(toml_module, "loads"))
    parsed = loads(contents.decode())
    config = cast(dict[str, object], parsed.get("config"))
    name = config.get("name")
    if not isinstance(name, str):
        raise TypeError("Asset minion config requires config.name")
    return AssetMinionConfig(name=name)


class SpiedMinion(SpyMixin, Minion[T_Event, T_Ctx], defer_minion_setup=True):
    _mn_user_facing = True

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)

    config: AssetMinionConfig

    async def load_config(self, config_path: str) -> AssetMinionConfig:
        return await load_asset_minion_config(config_path)