import asyncio
from typing import Any

from minions import minion_step
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.user_guarantees.persisted_shapes import DataclassContext, DataclassEvent


class DataclassPersistenceGuaranteeMinion(SpiedMinion[DataclassEvent, DataclassContext]):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        setattr(self, "_mn_shutdown_grace_seconds", 0.05)

    @minion_step
    async def step_1(self) -> None:
        self.context.seen_kind = self.event.kind
        self.context.seen_value = self.event.payload_value
        if self.context.seen_kind != "dataclass-event" or self.context.seen_value != 10:
            raise RuntimeError("dataclass event/context fields were not restored")
        await asyncio.sleep(0.2)


minion = DataclassPersistenceGuaranteeMinion
