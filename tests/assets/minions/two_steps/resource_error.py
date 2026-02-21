from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.event_counter import CounterEvent
from tests.assets.resources.error.runtime_error import ErrorResource


class ErrorResourceMinion(SpiedMinion[CounterEvent, dict]):
    name = "error-resource-minion"
    error_resource: ErrorResource

    @minion_step
    async def step_1(self):
        await self.error_resource.explode()


minion = ErrorResourceMinion
