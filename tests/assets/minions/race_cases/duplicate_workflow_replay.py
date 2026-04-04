import asyncio

from minions import minion_step

from tests.assets.events.counter import CounterEvent
from tests.assets.support.minion_spied import SpiedMinion


class DuplicateWorkflowReplayMinion(SpiedMinion[CounterEvent, dict]):
    name = "startup-replay-race-minion"
    _startup_entered: asyncio.Event | None = None
    _step_1_started: asyncio.Event | None = None
    _allow_step_1_finish: asyncio.Event | None = None

    @classmethod
    def reset_gates(cls) -> None:
        cls._startup_entered = asyncio.Event()
        cls._step_1_started = asyncio.Event()
        cls._allow_step_1_finish = asyncio.Event()

    @classmethod
    def _gate(cls, name: str) -> asyncio.Event:
        gate = getattr(cls, name)
        if gate is None:
            raise RuntimeError(f"{cls.__name__}.{name} must be initialized via reset_gates()")
        return gate

    async def startup(self) -> None:
        type(self)._gate("_startup_entered").set()

    @minion_step
    async def step_1(self) -> None:
        type(self)._gate("_step_1_started").set()
        await type(self)._gate("_allow_step_1_finish").wait()


minion = DuplicateWorkflowReplayMinion
