import asyncio
import pytest
from unittest.mock import AsyncMock

from minions._internal._utils.safe_create_task import safe_create_task
from minions._internal._framework.logger import ERROR

@pytest.mark.asyncio
async def test_safe_create_task_logs_on_exception():
    logger = AsyncMock()
    
    async def faulty():
        raise ValueError("test failure")

    task = safe_create_task(faulty(), logger, name="test_coro")
    await task

    assert logger._log.call_count == 1
    level, msg, kwargs = logger._log.call_args.args[0], logger._log.call_args.args[1], logger._log.call_args.kwargs
    assert level == ERROR
    assert "[Exception in asyncio.Task] (test_coro): test failure" in msg
    assert "traceback" in kwargs

@pytest.mark.asyncio
async def test_safe_create_task_success_does_not_log():
    logger = AsyncMock()

    async def okay():
        return 42

    task = safe_create_task(okay(), logger)
    await task

    logger._log.assert_not_called()

@pytest.mark.asyncio
async def test_safe_create_task_propagates_cancelled_error():
    logger = AsyncMock()

    async def cancellable():
        raise asyncio.CancelledError()

    task = safe_create_task(cancellable(), logger)

    with pytest.raises(asyncio.CancelledError):
        await task

    logger._log.assert_not_called()
