import asyncio
from typing import Any, ClassVar

from minions import minion_id, minion_step
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


@minion_id("82345678-1234-5678-9234-567812345678")
class AssetMinion(SpiedMinion[CounterEvent, CounterContext]):
    """Keep a healthy workflow in step one and stall cancellation until released."""

    step_entered: ClassVar[asyncio.Event] = asyncio.Event()
    cancellation_stalled: ClassVar[asyncio.Event] = asyncio.Event()
    allow_cancellation: ClassVar[asyncio.Event] = asyncio.Event()
    step_exited: ClassVar[asyncio.Event] = asyncio.Event()

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        setattr(self, "_mn_shutdown_grace_seconds", 0.01)

    @minion_step
    async def step_1(self) -> None:
        type(self).step_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            type(self).cancellation_stalled.set()
            await type(self).allow_cancellation.wait()
            raise
        finally:
            type(self).step_exited.set()


minion = AssetMinion
