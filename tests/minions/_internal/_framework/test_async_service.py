# pyright: reportUnusedClass=false

import asyncio
from unittest.mock import AsyncMock

import pytest

from minions._internal._domain.exceptions import (
    MinionsError,
    TaskCancellationTimeoutError,
)
from minions._internal._framework.async_service import AsyncService, _AsyncServiceState
from minions._internal._framework.logger_noop import NoOpLogger
from tests.support.task_with_stalled_cancellation import (
    task_with_stalled_cancellation,
)


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


class FailingStartupService(StateTrackingService):
    async def startup(self) -> None:
        raise RuntimeError("startup failed")


@pytest.mark.asyncio
async def test_running_service_stops_when_service_task_is_cancelled():
    service = StateTrackingService()
    service_task = asyncio.create_task(service._mn_serve())
    await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

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
async def test_starting_service_stops_when_startup_fails():
    service = FailingStartupService()

    with pytest.raises(
        MinionsError,
        match=r"FailingStartupService\.startup failed",
    ):
        await asyncio.wait_for(service._mn_serve(), timeout=1.0)

    assert service.state_history == [
        _AsyncServiceState.CREATED,
        _AsyncServiceState.STARTING,
        _AsyncServiceState.STOPPING,
        _AsyncServiceState.STOPPED,
    ]


class TestWaitUntilRunning:
    @pytest.mark.asyncio
    async def test_returns_once_service_is_running(self):
        service = NoOpService()
        service_task = asyncio.create_task(service._mn_serve())

        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)
        assert service._mn_state is _AsyncServiceState.RUNNING
        assert not service_task.done()

        service_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(service_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_propagates_service_task_cancellation_reason_after_service_stops(
        self,
    ):
        service = NoOpService()
        service_task = asyncio.create_task(service._mn_serve())
        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

        service_task.cancel("service stopped")
        with pytest.raises(asyncio.CancelledError, match="service stopped"):
            await asyncio.wait_for(service_task, timeout=1.0)

        assert service._mn_state is _AsyncServiceState.STOPPED
        with pytest.raises(asyncio.CancelledError, match="service stopped"):
            await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

    @pytest.mark.asyncio
    async def test_propagates_startup_failure(self):
        service = FailingStartupService()
        service_task = asyncio.create_task(service._mn_serve())

        with pytest.raises(
            MinionsError,
            match=r"FailingStartupService\.startup failed",
        ):
            await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)
        with pytest.raises(
            MinionsError,
            match=r"FailingStartupService\.startup failed",
        ):
            await asyncio.wait_for(service_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_waiter_cancellation_does_not_prevent_service_start(self):
        service = NoOpService()
        wait_task = asyncio.create_task(service._mn_wait_until_running())
        await asyncio.sleep(0)

        wait_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(wait_task, timeout=1.0)

        assert service._mn_state is _AsyncServiceState.CREATED
        assert not service._mn_start_done.is_set()

        service_task = asyncio.create_task(service._mn_serve())
        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)
        assert service._mn_state is _AsyncServiceState.RUNNING

        service_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(service_task, timeout=1.0)


class TestWaitUntilRunningWithRunExitBeforeCompletion:
    @pytest.mark.asyncio
    async def test_return_raises_minions_error(self):
        class ReturningRunService(NoOpService):
            async def run(self) -> None:
                return

        service = ReturningRunService()
        service_task = asyncio.create_task(service._mn_serve())

        with pytest.raises(
            MinionsError,
            match=r"ReturningRunService\.run returned unexpectedly",
        ):
            await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)
        with pytest.raises(
            MinionsError,
            match=r"ReturningRunService\.run returned unexpectedly",
        ):
            await asyncio.wait_for(service_task, timeout=1.0)

    @pytest.mark.asyncio
    async def test_failure_raises_minions_error(self):
        class FailingRunService(NoOpService):
            async def run(self) -> None:
                raise RuntimeError("run failed")

        service = FailingRunService()
        service_task = asyncio.create_task(service._mn_serve())

        with pytest.raises(
            MinionsError,
            match=r"FailingRunService\.run failed",
        ):
            await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)
        with pytest.raises(
            MinionsError,
            match=r"FailingRunService\.run failed",
        ):
            await asyncio.wait_for(service_task, timeout=1.0)


class TestWaitUntilRunningWithRunExitAfterCompletion:
    @pytest.mark.asyncio
    async def test_return_raises_minions_error(self):
        class GatedReturningRunService(NoOpService):
            def __init__(self) -> None:
                super().__init__()
                self.finish_run = asyncio.Event()

            async def run(self) -> None:
                await self.finish_run.wait()

        service = GatedReturningRunService()
        service_task = asyncio.create_task(service._mn_serve())
        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

        service.finish_run.set()
        with pytest.raises(
            MinionsError,
            match=r"GatedReturningRunService\.run returned unexpectedly",
        ):
            await asyncio.wait_for(service_task, timeout=1.0)
        with pytest.raises(
            MinionsError,
            match=r"GatedReturningRunService\.run returned unexpectedly",
        ):
            await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

    @pytest.mark.asyncio
    async def test_failure_raises_minions_error(self):
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
        service_task = asyncio.create_task(service._mn_serve())
        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

        assert service.run_started.is_set()
        service.fail_run.set()
        with pytest.raises(MinionsError, match=r"GatedFailingRunService\.run failed"):
            await asyncio.wait_for(service_task, timeout=1.0)
        with pytest.raises(MinionsError, match=r"GatedFailingRunService\.run failed"):
            await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)


class TestServiceOwnedTasks:
    @pytest.mark.asyncio
    async def test_safe_create_task_forwards_failure_and_name_to_service_task_failure_hook(
        self,
    ):
        service = NoOpService()
        service._mn_on_service_task_failure = AsyncMock()
        task_failure = ValueError("boom")

        async def faulty():
            raise task_failure

        task = service.safe_create_task(faulty(), name="faulty")
        await task

        service._mn_on_service_task_failure.assert_called_once()
        service._mn_on_service_task_failure.assert_awaited_once_with(
            task_failure,
            "faulty",
        )

    @pytest.mark.asyncio
    async def test_shutdown_drains_task_created_on_next_loop_tick(self):
        service = NoOpService()
        loop = asyncio.get_running_loop()
        late_tasks: list[asyncio.Task[None]] = []

        def create_late_task():
            late_tasks.append(
                service.safe_create_task(asyncio.sleep(60), name="late-task")
            )

        def schedule_late_task():
            loop.call_soon(create_late_task)

        await service._mn_shutdown(post=schedule_late_task)

        assert len(late_tasks) == 1
        assert late_tasks[0].cancelled()
        async with service._mn_tasks_gate:
            assert not service._mn_service_tasks

    @pytest.mark.asyncio
    async def test_shutdown_raises_cancellation_timeout_and_service_forgets_stalled_task(
        self,
    ):
        service = NoOpService()
        service._mn_component_owned_task_cancellation_timeout_seconds = 0.02

        async with task_with_stalled_cancellation(
            name="stalled-task",
            task_factory=service.safe_create_task,
        ) as task:
            with pytest.raises(
                TaskCancellationTimeoutError,
                match="stalled-task",
            ):
                await service._mn_shutdown()
            async with service._mn_tasks_gate:
                assert task not in service._mn_service_tasks
            assert not task.done()


class TestEnsureShutdown:
    @pytest.mark.asyncio
    async def test_does_nothing_before_service_starts(self):
        service = NoOpService()
        service._mn_shutdown = AsyncMock()

        await service._mn_ensure_shutdown()

        assert service._mn_state is _AsyncServiceState.CREATED
        assert service._mn_shutdown_task is None
        service._mn_shutdown.assert_not_called()

    @pytest.mark.asyncio
    async def test_concurrent_callers_wait_for_single_shutdown_attempt(self):
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
        service_task = asyncio.create_task(service._mn_serve())
        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

        service_task.cancel()
        await asyncio.wait_for(service.shutdown_entered.wait(), timeout=1.0)

        first = asyncio.create_task(service._mn_ensure_shutdown())
        second = asyncio.create_task(service._mn_ensure_shutdown())

        await asyncio.sleep(0)
        assert service._mn_state is _AsyncServiceState.STOPPING
        assert not service_task.done()
        assert not first.done()
        assert not second.done()

        service.allow_shutdown.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0)
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(service_task, timeout=1.0)

        assert service._mn_state is _AsyncServiceState.STOPPED
        assert service.shutdown_calls == 1

    @pytest.mark.asyncio
    async def test_repeated_calls_raise_suppressed_shutdown_failure_without_retrying(
        self,
    ):
        class FailingShutdownService(NoOpService):
            def __init__(self) -> None:
                super().__init__()
                self.shutdown_calls = 0

            async def shutdown(self) -> None:
                self.shutdown_calls += 1
                raise RuntimeError("shutdown boom")

        service = FailingShutdownService()
        service_task = asyncio.create_task(service._mn_serve())
        await asyncio.wait_for(service._mn_wait_until_running(), timeout=1.0)

        service_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(service_task, timeout=1.0)

        with pytest.raises(
            MinionsError,
            match="FailingShutdownService.shutdown failed",
        ):
            await service._mn_ensure_shutdown()
        with pytest.raises(
            MinionsError,
            match="FailingShutdownService.shutdown failed",
        ):
            await service._mn_ensure_shutdown()

        assert service._mn_state is _AsyncServiceState.STOPPED
        assert service.shutdown_calls == 1
