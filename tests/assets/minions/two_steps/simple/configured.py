from minions import minion_step
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.minion_spied_configed import ConfiguredSpiedMinion


class ConfiguredSimpleMinion(ConfiguredSpiedMinion[SimpleEvent, SimpleContext]):

    @minion_step
    async def step_1(self) -> None:
        self.context.step1 = "step1"

    @minion_step
    async def step_2(self) -> None:
        self.context.step2 = "step2"


minion = ConfiguredSimpleMinion
