from minions import minion_step
from minions._internal._domain.exceptions import AbortWorkflow

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets_revamp.support.event_counter import CounterEvent


class AbortStepMinion(SpiedMinion[CounterEvent, dict]):
    name = "abort-step-minion"

    @minion_step
    async def step_1(self):
        raise AbortWorkflow("intentional abort")


minion = AbortStepMinion
