import asyncio
import pytest
from unittest.mock import AsyncMock

from minions._internal._utils.safe_create_task import safe_create_task
from minions._internal._framework.logger import ERROR
from minions._internal._framework.logger import Logger

@pytest.mark.asyncio
async def test_safe_create_task_logs_on_exception():
    logger = AsyncMock(spec=Logger)
    
    async def faulty():
        raise ValueError("test failure")

    task = safe_create_task(faulty(), logger, name="test_coro")
    await task

    logger._log.assert_called_once()

    level = logger._log.call_args.args[0]
    msg = logger._log.call_args.args[1]
    kwargs = logger._log.call_args.kwargs
    
    assert level == ERROR
    assert "[Exception in asyncio.Task] (test_coro): test failure" in msg
    assert "traceback" in kwargs

@pytest.mark.asyncio
async def test_safe_create_task_success_does_not_log():
    logger = AsyncMock(spec=Logger)

    async def okay():
        return 42

    task = safe_create_task(okay(), logger)
    await task

    logger._log.assert_not_called()

@pytest.mark.asyncio
async def test_safe_create_task_propagates_cancelled_error():
    logger = AsyncMock(spec=Logger)

    async def cancellable():
        raise asyncio.CancelledError()

    task = safe_create_task(cancellable(), logger)

    with pytest.raises(asyncio.CancelledError):
        await task

    logger._log.assert_not_called()

@pytest.mark.asyncio
async def test_safe_create_task_swallows_logger_failures():
    logger = AsyncMock(spec=Logger)
    logger._log.side_effect = RuntimeError("logger backend failed")

    async def faulty():
        raise ValueError("boom")

    task = safe_create_task(faulty(), logger, name="faulty_with_bad_logger")
    await task

    logger._log.assert_called()

@pytest.mark.asyncio
async def test_safe_create_task_calls_on_failure_for_user_exceptions():
    logger = AsyncMock(spec=Logger)
    on_failure = AsyncMock()

    async def faulty():
        raise ValueError("boom")

    task = safe_create_task(faulty(), logger, name="faulty_task", on_failure=on_failure)
    await task

    on_failure.assert_called_once()

    exception, task_name, tb = on_failure.call_args.args

    assert isinstance(exception, ValueError)
    assert str(exception) == "boom"
    assert task_name == "faulty_task"
    assert "ValueError: boom" in tb

@pytest.mark.asyncio
async def test_safe_create_task_calls_on_failure_for_system_exit():
    logger = AsyncMock(spec=Logger)
    on_failure = AsyncMock()

    async def exits():
        raise SystemExit("bye")

    task = safe_create_task(exits(), logger, name="system_exit_task", on_failure=on_failure)
    await task

    on_failure.assert_called_once()
    exception, task_name, tb = on_failure.call_args.args
    assert isinstance(exception, SystemExit)
    assert str(exception) == "bye"
    assert task_name == "system_exit_task"
    assert "SystemExit: bye" in tb

@pytest.mark.asyncio
async def test_safe_create_task_swallows_on_failure_errors():
    logger = AsyncMock(spec=Logger)

    async def bad_failure_hook(*_args, **_kwargs):
        raise RuntimeError("failure hook crashed")

    async def faulty():
        raise ValueError("boom")

    task = safe_create_task(faulty(), logger, name="faulty_with_bad_hook", on_failure=bad_failure_hook)
    await task

    logger._log.assert_called()

    msg = logger._log.call_args.args[1]
    kwargs = logger._log.call_args.kwargs

    assert "[safe_create_task on_failure failed] (faulty_with_bad_hook): failure hook crashed" in msg
    assert "traceback" in kwargs
