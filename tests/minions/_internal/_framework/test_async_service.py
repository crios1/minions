import asyncio
from unittest.mock import AsyncMock

import pytest

from minions._internal._domain.exceptions import MinionsError
from minions._internal._framework.async_service import AsyncService
from minions._internal._framework.logger_noop import NoOpLogger


class NoOpService(AsyncService):
    def __init__(self) -> None:
        super().__init__(NoOpLogger())


@pytest.mark.asyncio
async def test_default_run_remains_active_until_cancelled():
    service = NoOpService()
    run_task = asyncio.create_task(service.run())
    await asyncio.sleep(0)

    assert not run_task.done()

    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(run_task, timeout=1.0)


@pytest.mark.asyncio
async def test_wait_until_started_sets_and_waits():
    service = NoOpService()

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
    service = NoOpService()
    service._mn_start_error = RuntimeError("start failed")
    service._mn_started.set()
    with pytest.raises(RuntimeError, match="start failed"):
        await service._mn_wait_until_started()


@pytest.mark.asyncio
async def test_safe_create_task_invokes_service_task_failure_hook():
    service = NoOpService()
    service._mn_on_service_task_failure = AsyncMock()  # type: ignore[method-assign]

    async def faulty():
        raise ValueError("boom")

    task = service.safe_create_task(faulty(), name="faulty")
    await task

    service._mn_on_service_task_failure.assert_awaited_once()  # type: ignore[attr-defined]
    err, task_name = service._mn_on_service_task_failure.await_args.args  # type: ignore[attr-defined]
    assert isinstance(err, ValueError)
    assert task_name == "faulty"


@pytest.mark.asyncio
async def test_shutdown_drains_tracked_task_scheduled_next_tick():
    service = NoOpService()

    loop = asyncio.get_running_loop()

    def schedule_late_task():
        loop.call_soon(service.safe_create_task, asyncio.sleep(60), "late-task")

    await service._mn_shutdown(post=schedule_late_task)

    async with service._mn_tasks_gate:
        assert not service._mn_service_tasks


@pytest.mark.asyncio
async def test_ensure_shutdown_starts_once_and_all_callers_wait_for_completion():
    class GatedShutdownService(NoOpService):
        def __init__(self) -> None:
            super().__init__()
            self.shutdown_calls = 0
            self.shutdown_entered = asyncio.Event()
            self.allow_shutdown = asyncio.Event()

        async def shutdown(self) -> None:
            self.shutdown_calls += 1
            self.shutdown_entered.set()
            await self.allow_shutdown.wait()

    service = GatedShutdownService()
    first = asyncio.create_task(service._mn_ensure_shutdown())
    second = asyncio.create_task(service._mn_ensure_shutdown())

    await service.shutdown_entered.wait()
    await asyncio.sleep(0)
    assert not first.done()
    assert not second.done()

    service.allow_shutdown.set()
    await asyncio.gather(first, second)

    assert service.shutdown_calls == 1


@pytest.mark.asyncio
async def test_ensure_shutdown_replays_same_failure_without_retrying():
    class FailingShutdownService(NoOpService):
        def __init__(self) -> None:
            super().__init__()
            self.shutdown_calls = 0

        async def shutdown(self) -> None:
            self.shutdown_calls += 1
            raise RuntimeError("shutdown boom")

    service = FailingShutdownService()

    with pytest.raises(MinionsError, match="FailingShutdownService.shutdown failed") as first:
        await service._mn_ensure_shutdown()
    with pytest.raises(MinionsError, match="FailingShutdownService.shutdown failed") as second:
        await service._mn_ensure_shutdown()

    assert service.shutdown_calls == 1
    assert first.value is second.value
