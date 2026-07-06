import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from minions._internal._framework.logger import ERROR
from minions._internal._utils.safe_create_task import safe_create_task
from tests.assets.support.logger_inmemory import InMemoryLogger


class FailingLogger(InMemoryLogger):
    async def log(self, level: int, msg: str, **kwargs: Any) -> None:
        raise RuntimeError("logger backend failed")


@pytest.mark.asyncio
async def test_safe_create_task_logs_on_exception(logger: InMemoryLogger):
    async def faulty() -> None:
        raise ValueError("test failure")

    task = safe_create_task(faulty(), logger, name="test_coro")
    await task

    [log] = logger.logs

    assert log.level == ERROR
    assert "[Exception in asyncio.Task] (test_coro): test failure" in log.msg
    assert log.kwargs["error_type"] == "ValueError"
    assert log.kwargs["error_message"] == "test failure"
    assert "traceback" in log.kwargs


@pytest.mark.asyncio
async def test_safe_create_task_success_does_not_log(logger: InMemoryLogger):
    async def okay() -> int:
        return 42

    task = safe_create_task(okay(), logger)
    await task

    assert logger.logs == []


@pytest.mark.asyncio
async def test_safe_create_task_propagates_cancelled_error(logger: InMemoryLogger):
    async def cancellable() -> None:
        raise asyncio.CancelledError()

    task = safe_create_task(cancellable(), logger)

    with pytest.raises(asyncio.CancelledError):
        await task

    assert logger.logs == []


@pytest.mark.asyncio
async def test_safe_create_task_swallows_logger_failures() -> None:
    logger = FailingLogger()

    async def faulty() -> None:
        raise ValueError("boom")

    task = safe_create_task(faulty(), logger, name="faulty_with_bad_logger")
    await task


@pytest.mark.asyncio
async def test_safe_create_task_calls_on_failure_for_user_exceptions(
    logger: InMemoryLogger,
):
    on_failure = AsyncMock()

    async def faulty() -> None:
        raise ValueError("boom")

    task = safe_create_task(faulty(), logger, name="faulty_task", on_failure=on_failure)
    await task

    on_failure.assert_called_once()

    exception, task_name = on_failure.call_args.args

    assert isinstance(exception, ValueError)
    assert str(exception) == "boom"
    assert task_name == "faulty_task"


@pytest.mark.asyncio
async def test_safe_create_task_calls_on_failure_for_system_exit(
    logger: InMemoryLogger,
):
    on_failure = AsyncMock()

    async def exits() -> None:
        raise SystemExit("bye")

    task = safe_create_task(exits(), logger, name="system_exit_task", on_failure=on_failure)
    await task

    on_failure.assert_called_once()
    exception, task_name = on_failure.call_args.args
    assert isinstance(exception, SystemExit)
    assert str(exception) == "bye"
    assert task_name == "system_exit_task"


@pytest.mark.asyncio
async def test_safe_create_task_swallows_on_failure_errors(logger: InMemoryLogger):
    async def bad_failure_hook(*_args: object, **_kwargs: object):
        raise RuntimeError("failure hook crashed")

    async def faulty() -> None:
        raise ValueError("boom")

    task = safe_create_task(
        faulty(),
        logger,
        name="faulty_with_bad_hook",
        on_failure=bad_failure_hook
    )
    await task

    log = logger.logs[-1]

    assert (
        "[safe_create_task on_failure failed] (faulty_with_bad_hook): failure hook crashed"
        in log.msg
    )
    assert log.kwargs["error_type"] == "RuntimeError"
    assert log.kwargs["error_message"] == "failure hook crashed"
    assert "traceback" in log.kwargs
