from minions import minion_step
from minions._internal._domain.exceptions import AbortWorkflow

from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent


class AbortStepMinion(SpiedMinion[CounterEvent, CounterContext]):
    name = "abort-step-minion"

    @minion_step
    async def step_1(self) -> None:
        raise AbortWorkflow("intentional abort")


minion = AbortStepMinion
