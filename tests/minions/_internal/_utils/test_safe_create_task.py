import asyncio
from unittest.mock import AsyncMock

import pytest

from minions._internal._utils.safe_create_task import safe_create_task

# Task Safety Contract


@pytest.mark.asyncio
async def test_contains_task_failure():
    def ignore_failure(exception: BaseException, task_name: str | None) -> None:
        pass

    async def faulty() -> None:
        raise ValueError("boom")

    supervision_task = safe_create_task(faulty(), on_failure=ignore_failure)

    await supervision_task

    assert supervision_task.done()
    assert not supervision_task.cancelled()
    assert supervision_task.exception() is None


# Failure Handler Dispatch


@pytest.mark.asyncio
async def test_does_not_call_failure_handler_on_task_success():
    on_failure = AsyncMock()

    async def okay() -> int:
        return 42

    await safe_create_task(okay(), on_failure=on_failure)

    on_failure.assert_not_called()


@pytest.mark.asyncio
async def test_calls_and_awaits_async_failure_handler_once_on_task_failure():
    on_failure = AsyncMock()

    async def faulty() -> None:
        raise ValueError("boom")

    await safe_create_task(faulty(), on_failure=on_failure)

    on_failure.assert_called_once()
    on_failure.assert_awaited_once()


@pytest.mark.asyncio
async def test_calls_sync_failure_handler_once_on_task_failure():
    call_count = 0

    def on_failure(exception: BaseException, task_name: str | None) -> None:
        nonlocal call_count
        call_count += 1

    async def faulty() -> None:
        raise ValueError("boom")

    await safe_create_task(faulty(), on_failure=on_failure)

    assert call_count == 1


# TaskFailureHandler Protocol


@pytest.mark.asyncio
async def test_invokes_protocol_with_original_failure_and_explicit_name():
    received: list[tuple[BaseException, str | None]] = []
    task_failure = ValueError("boom")

    async def record_failure(
        exception: BaseException,
        task_name: str | None,
    ) -> None:
        received.append((exception, task_name))

    async def faulty() -> None:
        raise task_failure

    await safe_create_task(
        faulty(),
        on_failure=record_failure,
        name="faulty_task",
    )

    assert received == [(task_failure, "faulty_task")]


@pytest.mark.asyncio
async def test_invokes_protocol_with_non_cancellation_base_exception_and_inferred_name(
):
    on_failure = AsyncMock()
    error = SystemExit("bye")

    async def exits() -> None:
        raise error

    await safe_create_task(exits(), on_failure=on_failure)

    on_failure.assert_called_once_with(error, "exits")
    on_failure.assert_awaited_once_with(error, "exits")


# Failure Handler Failure Containment


@pytest.mark.asyncio
async def test_contains_failure_handler_exception_and_reports_both_failures_to_stderr(
    capsys: pytest.CaptureFixture[str],
):
    async def bad_failure_handler(
        exception: BaseException,
        task_name: str | None,
    ) -> None:
        raise RuntimeError("failure handler crashed")

    async def faulty() -> None:
        raise ValueError("boom")

    await safe_create_task(faulty(), on_failure=bad_failure_handler)

    stderr = capsys.readouterr().err
    assert "RuntimeError: failure handler crashed" in stderr
    assert "original task failure: ValueError: boom" in stderr


@pytest.mark.asyncio
async def test_contains_failure_handler_base_exception_and_reports_both_failures_to_stderr(
    capsys: pytest.CaptureFixture[str],
):
    async def exiting_failure_handler(
        exception: BaseException,
        task_name: str | None,
    ) -> None:
        raise SystemExit("failure handler requested exit")

    async def faulty() -> None:
        raise ValueError("boom")

    await safe_create_task(faulty(), on_failure=exiting_failure_handler)

    stderr = capsys.readouterr().err
    assert "SystemExit: failure handler requested exit" in stderr
    assert "original task failure: ValueError: boom" in stderr


# Cancellation


@pytest.mark.asyncio
async def test_task_cancellation_propagates_without_calling_failure_handler():
    on_failure = AsyncMock()

    async def cancelled() -> None:
        raise asyncio.CancelledError()

    with pytest.raises(asyncio.CancelledError):
        await safe_create_task(cancelled(), on_failure=on_failure)

    on_failure.assert_not_called()


@pytest.mark.asyncio
async def test_task_cancellation_interrupts_failure_handler_and_propagates():
    handler_started = asyncio.Event()

    async def waiting_failure_handler(
        exception: BaseException,
        task_name: str | None,
    ) -> None:
        handler_started.set()
        await asyncio.Event().wait()

    async def faulty() -> None:
        raise ValueError("boom")

    task = safe_create_task(faulty(), on_failure=waiting_failure_handler)
    await asyncio.wait_for(handler_started.wait(), timeout=1)

    task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await task

    assert task.cancelled()


@pytest.mark.asyncio
async def test_propagates_cancellation_raised_by_failure_handler():
    async def cancelling_failure_handler(
        exception: BaseException,
        task_name: str | None,
    ) -> None:
        raise asyncio.CancelledError()

    async def faulty() -> None:
        raise ValueError("boom")

    with pytest.raises(asyncio.CancelledError):
        await safe_create_task(faulty(), on_failure=cancelling_failure_handler)
