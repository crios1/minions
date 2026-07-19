import asyncio

import pytest

from tests.support.race_window import GatedLock


@pytest.mark.asyncio
async def test_gated_lock_holds_production_code_until_progress_is_allowed() -> None:
    lock = GatedLock()
    completed = False

    async def use_lock() -> None:
        nonlocal completed
        async with lock:
            completed = True

    task = asyncio.create_task(use_lock())
    await lock.wait_until_held()

    assert lock.locked()
    assert lock.enter_count == 1
    assert not completed
    assert not task.done()

    lock.allow_progress()
    await task

    assert completed
    assert not lock.locked()


@pytest.mark.asyncio
async def test_gated_lock_wait_times_out_before_lock_is_held() -> None:
    lock = GatedLock()

    with pytest.raises(TimeoutError):
        await lock.wait_until_held(timeout=0.001)
