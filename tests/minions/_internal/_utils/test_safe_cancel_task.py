import asyncio
import inspect
import pytest

from minions._internal._utils.safe_cancel_task import safe_cancel_task
from minions._internal._framework.logger import ERROR
from tests.assets.support.logger_inmemory import InMemoryLogger

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
async def test_logs_on_timeout_with_label_using_logger():
    async def stubborn():
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)

    logger = InMemoryLogger()
    t = asyncio.create_task(stubborn())
    await asyncio.sleep(0)
    await safe_cancel_task(t, label="worker-1", timeout=0.05, logger=logger)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    assert logger.logs, "expected a log call"
    entry = logger.logs[0]
    assert entry.level == ERROR
    assert "Timeout while cancelling task 'worker-1'" in entry.msg
    assert isinstance(entry.kwargs.get('traceback'), str) and entry.kwargs.get('traceback')

@pytest.mark.asyncio
async def test_prints_on_timeout_without_logger_default_label(capsys):
    async def stubborn():
        try:
            while True:
                await asyncio.sleep(0.01)
        except asyncio.CancelledError:
            await asyncio.sleep(0.2)

    t = asyncio.create_task(stubborn())
    await asyncio.sleep(0)
    await safe_cancel_task(t, timeout=0.05)
    t.cancel()
    with pytest.raises(asyncio.CancelledError):
        await t

    captured = capsys.readouterr()
    err = captured.err
    assert "Timeout while cancelling task" in err
    assert ("<no traceback>" in err) or ("File " in err)

