import asyncio
from unittest.mock import AsyncMock

import pytest

from minions._internal._domain.exceptions import MinionsError
from minions._internal._framework.async_service import AsyncService, _AsyncServiceState
from minions._internal._framework.logger_noop import NoOpLogger


class NoOpService(AsyncService):
    def __init__(self) -> None:
        super().__init__(NoOpLogger())


class StateTrackingService(NoOpService):
    def __init__(self) -> None:
        self.state_history: list[_AsyncServiceState] = []
        super().__init__()
        assert hasattr(self, "_mn_state"), "AsyncService._mn_state attribute drifted"

    def __setattr__(self, name: str, value: object) -> None:
        if name == "_mn_state" and isinstance(value, _AsyncServiceState):
            self.state_history.append(value)
        super().__setattr__(name, value)


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
async def test_successful_lifecycle_follows_valid_state_flow():
    service = StateTrackingService()
    service_task = asyncio.create_task(service._mn_start())
    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)

    service_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service_task, timeout=1.0)

    assert service.state_history == [
        _AsyncServiceState.CREATED,
        _AsyncServiceState.STARTING,
        _AsyncServiceState.RUNNING,
        _AsyncServiceState.STOPPING,
        _AsyncServiceState.STOPPED,
    ]


@pytest.mark.asyncio
async def test_startup_failure_follows_valid_state_flow():
    service = StateTrackingService()
    service._mn_startup = AsyncMock(  # type: ignore[method-assign]
        side_effect=RuntimeError("startup failed")
    )

    with pytest.raises(RuntimeError, match="startup failed"):
        await asyncio.wait_for(service._mn_start(), timeout=1.0)

    assert service.state_history == [
        _AsyncServiceState.CREATED,
        _AsyncServiceState.STARTING,
        _AsyncServiceState.STOPPING,
        _AsyncServiceState.STOPPED,
    ]


@pytest.mark.asyncio
async def test_wait_until_started_waits_for_active_run_handoff():
    service = NoOpService()
    service_task = asyncio.create_task(service._mn_start())

    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)
    assert service._mn_state is _AsyncServiceState.RUNNING
    assert not service_task.done()

    service_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service_task, timeout=1.0)
    assert service._mn_state is _AsyncServiceState.STOPPED
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)


@pytest.mark.asyncio
async def test_wait_until_started_raises_startup_failure():
    service = NoOpService()
    service._mn_startup = AsyncMock(side_effect=RuntimeError("startup failed"))  # type: ignore[method-assign]
    service_task = asyncio.create_task(service._mn_start())

    with pytest.raises(RuntimeError, match="startup failed"):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)
    with pytest.raises(RuntimeError, match="startup failed"):
        await asyncio.wait_for(service_task, timeout=1.0)


@pytest.mark.asyncio
async def test_wait_until_started_raises_if_run_returns_during_handoff():
    service = NoOpService()
    service._mn_run = AsyncMock(return_value=None)
    service_task = asyncio.create_task(service._mn_start())

    with pytest.raises(MinionsError, match=r"NoOpService\.run returned unexpectedly"):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)
    with pytest.raises(MinionsError, match=r"NoOpService\.run returned unexpectedly"):
        await asyncio.wait_for(service_task, timeout=1.0)


@pytest.mark.asyncio
async def test_wait_until_started_raises_if_run_fails_during_handoff():
    service = NoOpService()
    service._mn_run = AsyncMock(side_effect=RuntimeError("run failed"))
    service_task = asyncio.create_task(service._mn_start())

    with pytest.raises(RuntimeError, match="run failed"):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)
    with pytest.raises(RuntimeError, match="run failed"):
        await asyncio.wait_for(service_task, timeout=1.0)


@pytest.mark.asyncio
async def test_wait_until_started_raises_after_run_fails():
    class GatedFailingRunService(NoOpService):
        def __init__(self) -> None:
            super().__init__()
            self.run_started = asyncio.Event()
            self.fail_run = asyncio.Event()

        async def run(self) -> None:
            self.run_started.set()
            await self.fail_run.wait()
            raise RuntimeError("run failed")

    service = GatedFailingRunService()
    service_task = asyncio.create_task(service._mn_start())

    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)
    assert service.run_started.is_set()
    service.fail_run.set()
    with pytest.raises(MinionsError, match=r"GatedFailingRunService\.run failed"):
        await asyncio.wait_for(service_task, timeout=1.0)
    with pytest.raises(MinionsError, match=r"GatedFailingRunService\.run failed"):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)


@pytest.mark.asyncio
async def test_wait_until_started_raises_after_run_returns():
    class GatedReturningRunService(NoOpService):
        def __init__(self) -> None:
            super().__init__()
            self.finish_run = asyncio.Event()

        async def run(self) -> None:
            await self.finish_run.wait()

    service = GatedReturningRunService()
    service_task = asyncio.create_task(service._mn_start())

    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)
    service.finish_run.set()
    with pytest.raises(
        MinionsError, match=r"GatedReturningRunService\.run returned unexpectedly"
    ):
        await asyncio.wait_for(service_task, timeout=1.0)
    with pytest.raises(
        MinionsError, match=r"GatedReturningRunService\.run returned unexpectedly"
    ):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)


@pytest.mark.asyncio
async def test_cancelling_readiness_wait_does_not_affect_service_lifecycle():
    service = NoOpService()
    wait_task = asyncio.create_task(service._mn_wait_until_started())
    await asyncio.sleep(0)

    wait_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(wait_task, timeout=1.0)

    assert service._mn_state is _AsyncServiceState.CREATED
    assert not service._mn_start_resolved.is_set()

    service_task = asyncio.create_task(service._mn_start())
    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)

    service_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service_task, timeout=1.0)


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
async def test_ensure_shutdown_does_nothing_before_service_starts():
    service = NoOpService()
    service._mn_shutdown = AsyncMock()

    await service._mn_ensure_shutdown()

    assert service._mn_state is _AsyncServiceState.CREATED
    assert service._mn_shutdown_task is None
    service._mn_shutdown.assert_not_called()


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
    service_task = asyncio.create_task(service._mn_start())
    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)

    service_task.cancel()
    await asyncio.wait_for(service.shutdown_entered.wait(), timeout=1.0)

    first = asyncio.create_task(service._mn_ensure_shutdown())
    second = asyncio.create_task(service._mn_ensure_shutdown())

    await asyncio.sleep(0)
    assert service._mn_state is _AsyncServiceState.STOPPING
    assert not first.done()
    assert not second.done()

    service.allow_shutdown.set()
    await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0)
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service_task, timeout=1.0)

    assert service._mn_state is _AsyncServiceState.STOPPED
    assert service.shutdown_calls == 1
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)


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
    service_task = asyncio.create_task(service._mn_start())
    await asyncio.wait_for(service._mn_wait_until_started(), timeout=1.0)

    service_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(service_task, timeout=1.0)

    with pytest.raises(MinionsError, match="FailingShutdownService.shutdown failed") as first:
        await service._mn_ensure_shutdown()
    with pytest.raises(MinionsError, match="FailingShutdownService.shutdown failed") as second:
        await service._mn_ensure_shutdown()

    assert service._mn_state is _AsyncServiceState.STOPPED
    assert service.shutdown_calls == 1
    assert first.value is second.value
