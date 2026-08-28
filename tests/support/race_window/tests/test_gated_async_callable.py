import asyncio

import pytest

from tests.support.race_window import GatedAsyncCallable


@pytest.mark.asyncio
async def test_blocks_until_return_is_allowed():
    gated_callable = GatedAsyncCallable(result="result")

    task = asyncio.create_task(gated_callable())
    await gated_callable.wait_until_called()

    assert gated_callable.call_count == 1
    assert not task.done()

    gated_callable.allow_return()

    assert await task == "result"


@pytest.mark.asyncio
async def test_wait_until_called_times_out_when_not_called():
    gated_callable = GatedAsyncCallable[None]()

    with pytest.raises(TimeoutError):
        await gated_callable.wait_until_called(timeout=0.001)


@pytest.mark.asyncio
async def test_allow_return_releases_future_calls():
    gated_callable = GatedAsyncCallable(result="result")
    gated_callable.allow_return()

    assert await gated_callable() == "result"
    assert gated_callable.call_count == 1
