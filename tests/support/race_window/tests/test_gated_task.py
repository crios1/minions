import pytest

from tests.support.race_window import GatedTask


@pytest.mark.asyncio
async def test_stays_pending_until_released():
    gated_task = GatedTask()

    assert not gated_task.done()

    gated_task.release()

    await gated_task


@pytest.mark.asyncio
async def test_release_is_idempotent():
    gated_task = GatedTask()

    gated_task.release()
    gated_task.release()

    await gated_task
