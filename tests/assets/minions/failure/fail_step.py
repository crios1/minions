from minions import minion_step

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.event_counter import CounterEvent


class FailStepMinion(SpiedMinion[CounterEvent, dict]):
    name = "fail-step-minion"

    @minion_step
    async def step_1(self):
        raise RuntimeError("intentional failure")


minion = FailStepMinion
