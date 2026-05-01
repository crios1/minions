import asyncio

from minions import minion_step
from tests.assets.user_guarantees.persisted_shapes import StructContext, StructEvent
from tests.assets.support.minion_spied import SpiedMinion


class StructPersistenceGuaranteeMinion(SpiedMinion[StructEvent, StructContext]):
    name = "struct-persistence-guarantee-minion"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self) -> None:
        self.context.seen_kind = self.event.kind
        self.context.seen_value = self.event.payload_value
        if self.context.seen_kind != "struct-event" or self.context.seen_value != 10:
            raise RuntimeError("msgspec event/context fields were not restored")
        await asyncio.sleep(0.2)


minion = StructPersistenceGuaranteeMinion
