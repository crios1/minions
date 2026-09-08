from typing import ClassVar

from minions import minion_step
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.user_guarantees.persisted_shapes import StructContext, StructEvent
from tests.support.race_window import WaiterThresholdGate


class AssetMinion(SpiedMinion[StructEvent, StructContext]):
    _step_1_waiter_gate: ClassVar[WaiterThresholdGate] = WaiterThresholdGate(
        threshold=2
    )

    @minion_step
    async def step_1(self) -> None:
        self.context.seen_kind = self.event.kind
        self.context.seen_value = self.event.payload_value
        if self.context.seen_kind != "struct-event" or self.context.seen_value != 10:
            raise RuntimeError("msgspec event/context fields were not restored")
        await type(self)._step_1_waiter_gate.wait_until_open()


minion = AssetMinion
