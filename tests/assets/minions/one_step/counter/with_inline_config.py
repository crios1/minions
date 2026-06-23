from dataclasses import dataclass

import msgspec

from minions import Minion, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent


@dataclass
class InlineDataclassConfig:
    name: str


class InlineStructConfig(msgspec.Struct):
    name: str


class AssetMinion(Minion[CounterEvent, CounterContext]):
    config: InlineDataclassConfig | InlineStructConfig

    @minion_step
    async def step_1(self) -> None:
        return


minion = AssetMinion
