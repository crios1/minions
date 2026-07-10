import asyncio
from typing import Any

import pytest

from minions import Minion, minion_step
from minions._internal._domain.exceptions import MinionsError
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore


class NoOpMinion(Minion[EmptyEvent, EmptyContext]):
    @minion_step
    async def step_1(self):
        return


def _make_no_op_minion(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> NoOpMinion:
    return NoOpMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_id="dummy-minion-id",
        minion_config_id="dummy-minion-config-id",
        pipeline_id="dummy-pipeline-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=state_store,
        metrics=metrics,
        logger=logger,
    )


async def _add_pending_service_task(
    minion: Minion[Any, Any],
    event: asyncio.Event,
) -> asyncio.Task[None]:
    async def _wait_for_event() -> None:
        await event.wait()

    task = asyncio.create_task(_wait_for_event())
    async with minion._mn_tasks_gate:
        minion._mn_service_tasks.add(task)

    task.add_done_callback(lambda t: minion._mn_service_tasks.discard(t))

    return task


@pytest.mark.asyncio
async def test_wait_until_tasks_idle_ignores_service_tasks_by_default(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    minion = _make_no_op_minion(logger, metrics, state_store)

    event = asyncio.Event()
    task = await _add_pending_service_task(minion, event)

    try:
        await minion._mn_wait_until_tasks_idle(
            timeout=0.01,
            timeout_msg="NoOpMinion tasks did not become idle within 0.01s",
        )
        assert not task.done()
    finally:
        event.set()
        await task


@pytest.mark.asyncio
async def test_wait_until_tasks_idle_can_include_service_tasks(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    minion = _make_no_op_minion(logger, metrics, state_store)

    event = asyncio.Event()
    task = await _add_pending_service_task(minion, event)

    try:
        with pytest.raises(
            TimeoutError,
            match="NoOpMinion tasks did not become idle within 0.01s",
        ):
            await minion._mn_wait_until_tasks_idle(
                timeout=0.01,
                include_service_tasks=True,
                timeout_msg="NoOpMinion tasks did not become idle within 0.01s",
            )
    finally:
        event.set()
        await task


class TestMinionLifecycle:
    @pytest.mark.asyncio
    async def test_minion_startup_failure_context_includes_minion_identity(
        self,
        logger: InMemoryLogger,
    ):
        class FailingStartupMinion(Minion[EmptyEvent, EmptyContext]):
            async def startup(self) -> None:
                raise RuntimeError("boom")

            @minion_step
            async def step_1(self): ...

        m = FailingStartupMinion(
            minion_instance_id="instance-1",
            orchestration_id="orchestration-1",
            minion_module_path="tests.assets.FailingStartupMinion",
            config_path=None,
            state_store=NoOpStateStore(),
            metrics=NoOpMetrics(),
            logger=logger,
            minion_id="stable-minion-id",
            minion_config_id="config-1",
            pipeline_id="pipeline-1",
        )

        with pytest.raises(MinionsError) as exc_info:
            await m._mn_startup()

        assert exc_info.value.context["minion_id"] == "stable-minion-id"
        assert exc_info.value.context["minion_instance_id"] == "instance-1"
        assert exc_info.value.context["minion_config_id"] == "config-1"
        assert exc_info.value.context["minion_module_path"] == "tests.assets.FailingStartupMinion"
