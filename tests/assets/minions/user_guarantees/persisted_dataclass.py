import asyncio

from minions import minion_step
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.user_guarantees.persisted_shapes import DataclassContext, DataclassEvent


class AssetMinion(SpiedMinion[DataclassEvent, DataclassContext]):
    @minion_step
    async def step_1(self) -> None:
        self.context.seen_kind = self.event.kind
        self.context.seen_value = self.event.payload_value
        if self.context.seen_kind != "dataclass-event" or self.context.seen_value != 10:
            raise RuntimeError("dataclass event/context fields were not restored")
        await asyncio.sleep(0.2)


minion = AssetMinion
