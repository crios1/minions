import asyncio
import pytest
from unittest.mock import AsyncMock

from minions._internal._framework.async_service import AsyncService
from minions._internal._framework.logger_noop import NoOpLogger

class NoOpService(AsyncService):
    async def run(self):
        pass

@pytest.mark.asyncio
async def test_wait_until_started_sets_and_waits():
    logger = NoOpLogger()
    service = NoOpService(logger)

    # Start _mn_wait_until_started in the background
    wait_task = asyncio.create_task(service._mn_wait_until_started())
    await asyncio.sleep(0.01)  # Let the task start and block
    assert not wait_task.done()

    # Set _started, should unblock
    service._mn_started.set()
    await wait_task
    assert wait_task.done()

@pytest.mark.asyncio
async def test_wait_until_started_raises_on_start_error():
    logger = NoOpLogger()
    service = NoOpService(logger)
    service._mn_start_error = RuntimeError("start failed")
    service._mn_started.set()
    with pytest.raises(RuntimeError, match="start failed"):
        await service._mn_wait_until_started()


@pytest.mark.asyncio
async def test_safe_create_task_invokes_aux_failure_hook():
    logger = NoOpLogger()
    service = NoOpService(logger)
    service._mn_on_aux_task_failure = AsyncMock()  # type: ignore[method-assign]

    async def faulty():
        raise ValueError("aux boom")

    task = service.safe_create_task(faulty(), name="aux-faulty")
    await task

    service._mn_on_aux_task_failure.assert_awaited_once()  # type: ignore[attr-defined]
    err, task_name, tb = service._mn_on_aux_task_failure.await_args.args  # type: ignore[attr-defined]
    assert isinstance(err, ValueError)
    assert task_name == "aux-faulty"
    assert "ValueError: aux boom" in tb


@pytest.mark.asyncio
async def test_shutdown_drains_aux_task_scheduled_next_tick():
    logger = NoOpLogger()
    service = NoOpService(logger)

    loop = asyncio.get_running_loop()

    def schedule_late_aux_task():
        loop.call_soon(service.safe_create_task, asyncio.sleep(60), "late-aux-task")

    await service._mn_shutdown(post=schedule_late_aux_task)

    async with service._mn_tasks_lock:
        assert not service._mn_aux_tasks
