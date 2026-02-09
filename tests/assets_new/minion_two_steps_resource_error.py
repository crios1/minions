from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from .event_counter import CounterEvent
from .resource_error import ErrorResource


class ErrorResourceMinion(SpiedMinion[CounterEvent, dict]):
    name = "error-resource-minion"
    error_resource: ErrorResource

    @minion_step
    async def step_1(self):
        await self.error_resource.explode()


minion = ErrorResourceMinion
