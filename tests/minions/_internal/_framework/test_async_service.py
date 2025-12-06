import asyncio
import pytest

from minions._internal._framework.async_service import AsyncService
from minions._internal._framework.logger_noop import NoOpLogger

class NoOpService(AsyncService):
    async def run(self):
        pass

@pytest.mark.asyncio
async def test_wait_until_started_sets_and_waits():
    logger = NoOpLogger()
    service = NoOpService(logger)

    # Start _wait_until_started in the background
    wait_task = asyncio.create_task(service._wait_until_started())
    await asyncio.sleep(0.01)  # Let the task start and block
    assert not wait_task.done()

    # Set _started, should unblock
    service._started.set()
    await wait_task
    assert wait_task.done()

@pytest.mark.asyncio
async def test_wait_until_started_raises_on_start_error():
    logger = NoOpLogger()
    service = NoOpService(logger)
    service._start_error = RuntimeError("start failed")
    service._started.set()
    with pytest.raises(RuntimeError, match="start failed"):
        await service._wait_until_started()
