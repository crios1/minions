import asyncio

import pytest

from tests.support.race_window import GatedAsyncCallable


@pytest.mark.asyncio
async def test_gated_async_callable_blocks_until_return_is_allowed() -> None:
    gated_callable = GatedAsyncCallable(result="result")

    task = asyncio.create_task(gated_callable())
    await gated_callable.wait_until_called()

    assert gated_callable.call_count == 1
    assert not task.done()

    gated_callable.allow_return()

    assert await task == "result"


@pytest.mark.asyncio
async def test_gated_async_callable_wait_times_out_before_call() -> None:
    gated_callable = GatedAsyncCallable[None]()

    with pytest.raises(TimeoutError):
        await gated_callable.wait_until_called(timeout=0.001)


@pytest.mark.asyncio
async def test_gated_async_callable_open_gate_allows_future_calls_to_return() -> None:
    gated_callable = GatedAsyncCallable(result="result")
    gated_callable.allow_return()

    assert await gated_callable() == "result"
    assert gated_callable.call_count == 1
