import asyncio
import inspect

import pytest

from minions._internal._framework.logger import ERROR
from minions._internal._utils.safe_cancel_task import safe_cancel_task
from minions.exceptions import TaskCancellationError, TaskCancellationTimeoutError
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.support.task_with_stalled_cancellation import (
    task_with_stalled_cancellation,
)


def test_task_cancellation_timeout_error_is_cancellation_error_and_timeout_error():
    error = TaskCancellationTimeoutError("worker", 1.0)

    assert isinstance(error, TaskCancellationError)
    assert isinstance(error, TimeoutError)


@pytest.mark.asyncio
async def test_returns_early_on_falsy_task():
    assert inspect.iscoroutinefunction(safe_cancel_task)
    await safe_cancel_task(None)  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_cancels_task_cleanly():
    async def work():
        await asyncio.sleep(1)

    t = asyncio.create_task(work())
    await safe_cancel_task(t, timeout=0.1)
    assert t.cancelled()


@pytest.mark.asyncio
async def test_logs_on_timeout_with_label_when_using_logger(logger: InMemoryLogger):
    async with task_with_stalled_cancellation() as task:
        with pytest.raises(TaskCancellationTimeoutError) as raised:
            await safe_cancel_task(
                task,
                label="worker",
                timeout=0.01,
                logger=logger,
            )

    assert raised.value.label == "worker"
    assert raised.value.timeout == 0.01

    assert logger.logs, "expected a log call"
    entry = logger.logs[0]
    assert entry.level == ERROR
    assert "Timeout while cancelling task 'worker'" in entry.msg
    assert "error_type" in entry.kwargs
    assert isinstance(entry.kwargs.get("traceback"), str) and entry.kwargs.get(
        "traceback"
    )
    assert isinstance(entry.kwargs.get("task_stack"), str) and entry.kwargs.get(
        "task_stack"
    )


@pytest.mark.asyncio
async def test_prints_on_timeout_when_logger_not_provided(
    capsys: pytest.CaptureFixture[str],
) -> None:
    async with task_with_stalled_cancellation() as task:
        with pytest.raises(TaskCancellationTimeoutError):
            await safe_cancel_task(task, label="worker", timeout=0.01)

    captured = capsys.readouterr()
    err = captured.err
    assert "Timeout while cancelling task 'worker'" in err
    assert ("<no traceback>" in err) or ("File " in err)


@pytest.mark.asyncio
async def test_uses_task_name_as_label_when_label_not_provided(
    logger: InMemoryLogger,
) -> None:
    async with task_with_stalled_cancellation(name="worker") as task:
        with pytest.raises(TaskCancellationTimeoutError) as raised:
            await safe_cancel_task(task, timeout=0.01, logger=logger)

    assert raised.value.label == "worker"


@pytest.mark.asyncio
async def test_timeout_is_respected_when_cancellation_stalls():
    async with task_with_stalled_cancellation() as task:
        loop = asyncio.get_running_loop()
        started = loop.time()
        with pytest.raises(TaskCancellationTimeoutError):
            await safe_cancel_task(task, label="stubborn", timeout=0.01)
        assert loop.time() - started < 0.15
        assert not task.done()
