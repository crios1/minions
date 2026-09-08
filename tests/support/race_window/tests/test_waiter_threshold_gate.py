import asyncio

import pytest

from tests.support.race_window import WaiterThresholdGate


@pytest.mark.asyncio
async def test_opens_when_threshold_waiters_overlap() -> None:
    gate = WaiterThresholdGate(threshold=2)

    first = asyncio.create_task(gate.wait_until_open())
    await asyncio.sleep(0)
    assert not first.done()

    second = asyncio.create_task(gate.wait_until_open())
    await asyncio.wait_for(asyncio.gather(first, second), timeout=1.0)


@pytest.mark.asyncio
async def test_canceled_waiter_no_longer_counts_toward_threshold() -> None:
    gate = WaiterThresholdGate(threshold=2)

    first = asyncio.create_task(gate.wait_until_open())
    await asyncio.sleep(0)
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first

    second = asyncio.create_task(gate.wait_until_open())
    await asyncio.sleep(0)
    assert not second.done()

    third = asyncio.create_task(gate.wait_until_open())
    await asyncio.wait_for(asyncio.gather(second, third), timeout=1.0)


@pytest.mark.asyncio
async def test_future_waiters_pass_after_gate_opens() -> None:
    gate = WaiterThresholdGate(threshold=1)

    await gate.wait_until_open()
    await asyncio.wait_for(gate.wait_until_open(), timeout=1.0)


@pytest.mark.parametrize("threshold", [0, -1])
def test_rejects_nonpositive_threshold(threshold: int) -> None:
    with pytest.raises(ValueError, match="threshold must be >= 1"):
        WaiterThresholdGate(threshold=threshold)
