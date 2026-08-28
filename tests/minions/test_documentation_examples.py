import asyncio
import time
from dataclasses import dataclass
from typing import ClassVar

import pytest

from minions import Gru, Minion, Pipeline, Resource, minion_step
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


@dataclass(frozen=True)
class Heartbeat:
    timestamp: float


@dataclass
class WorkflowCtx:
    user_id: str = "heartbeat-worker"
    retries: int = 0


class HeartbeatStore(Resource):
    saved: ClassVar[asyncio.Event | None] = None
    observed_heartbeats: ClassVar[list[Heartbeat]] = []

    @classmethod
    def reset(cls) -> None:
        cls.saved = asyncio.Event()
        cls.observed_heartbeats = []

    async def save(self, heartbeat: Heartbeat) -> None:
        type(self).observed_heartbeats.append(heartbeat)
        saved = type(self).saved
        assert saved is not None
        saved.set()


class HeartbeatPipeline(Pipeline[Heartbeat]):
    async def produce_event(self) -> Heartbeat:
        if getattr(self, "_documentation_example_emitted", False):
            await asyncio.Event().wait()
        await asyncio.sleep(0.05)
        self._documentation_example_emitted = True
        return Heartbeat(timestamp=time.time())


class PrintMinion(Minion[Heartbeat, WorkflowCtx]):
    store: HeartbeatStore
    observed_user_ids: ClassVar[list[str]] = []

    @minion_step
    async def record(self) -> None:
        await self.store.save(self.event)
        self.context.retries += 1
        type(self).observed_user_ids.append(self.context.user_id)


@pytest.mark.asyncio
async def test_getting_started_example_handles_a_live_event():
    HeartbeatStore.reset()
    PrintMinion.observed_user_ids = []
    logger = InMemoryLogger()
    state_store = InMemoryStateStore(logger=logger)
    gru = await Gru.create(
        state_store=state_store,
        logger=logger,
        metrics=InMemoryMetrics(logger=logger),
    )
    try:
        started = await gru.start_orchestration(
            pipeline=HeartbeatPipeline,
            minion=PrintMinion,
        )
        assert started.success
        assert started.orchestration_id is not None
        saved = HeartbeatStore.saved
        assert saved is not None
        await asyncio.wait_for(saved.wait(), timeout=1.0)
        assert await logger.wait_for_log("Workflow succeeded", timeout=1.0)
        assert HeartbeatStore.observed_heartbeats
        assert PrintMinion.observed_user_ids == ["heartbeat-worker"]

        stopped = await gru.stop_orchestration(started.orchestration_id)
        assert stopped.success
        assert await state_store.get_all_contexts() == []
        assert (await gru.runtime_state_snapshot()).is_empty
    finally:
        await gru.shutdown()
