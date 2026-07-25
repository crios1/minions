from minions import minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.crash.resources.depends_on_boom_run import (
    AssetResource as DependsOnBoomRunResource,
)
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    depends_on_boom_run_resource: DependsOnBoomRunResource

    @minion_step
    async def step_1(self) -> None:
        self.context.seq = self.event.seq


minion = AssetMinion
