from minions import Minion, minion_step

from tests.assets.contexts.counter import CounterContext
from tests.assets.events.record import RecordEvent
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.minion_spied import AssetMinionConfig, load_asset_minion_config


class Minion1(Minion[RecordEvent, CounterContext]):
    @minion_step
    async def step_1():
        ...


class Minion2(Minion[SimpleEvent, CounterContext]):
    config: AssetMinionConfig

    async def load_config(self, config_path: str) -> AssetMinionConfig:
        return await load_asset_minion_config(config_path)

    @minion_step
    async def step_1():
        ...


minion = Minion2
