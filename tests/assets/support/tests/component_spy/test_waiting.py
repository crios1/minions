import asyncio
from typing import Any

import pytest

from tests.assets.support.component_spy_meta import ComponentSpyMeta


@pytest.mark.asyncio
async def test_wait_for_call_returns_immediately_when_requested_count_was_already_reached():
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    await component.method()

    await SpiedComponent.wait_for_call("method", count=1, timeout=0)


@pytest.mark.asyncio
async def test_wait_for_call_resolves_multiple_waiters_at_their_requested_counts():
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    first_waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", count=1, timeout=1.0))
    second_waiter = asyncio.create_task(
        SpiedComponent.wait_for_call("method", count=2, timeout=1.0)
    )
    await asyncio.sleep(0)

    await component.method()
    await first_waiter
    assert not second_waiter.done()

    await component.method()
    await second_waiter


@pytest.mark.asyncio
async def test_wait_for_calls_resolves_after_requested_count_is_reached():
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    waiter = asyncio.create_task(SpiedComponent.wait_for_calls({"method": 2}, timeout=1.0))
    await component.method()
    assert not waiter.done()
    await component.method()
    await waiter


@pytest.mark.asyncio
async def test_wait_for_call_resolves_when_async_method_call_starts_not_when_it_finishes():
    call_can_finish = asyncio.Event()

    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            await call_can_finish.wait()

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    call = asyncio.create_task(component.method())
    await SpiedComponent.wait_for_call("method", timeout=1.0)
    assert not call.done()

    call_can_finish.set()
    await call


@pytest.mark.asyncio
async def test_cancelling_wait_for_call_does_not_disrupt_later_wait():
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    cancelled_waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", timeout=1.0))
    await asyncio.sleep(0)
    cancelled_waiter.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_waiter

    next_waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", timeout=1.0))
    await asyncio.sleep(0)
    await component.method()
    await next_waiter


@pytest.mark.asyncio
async def test_observes_sync_method_call_from_another_thread():
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    await asyncio.to_thread(component.method)

    assert SpiedComponent.get_call_counts() == {"method": 1}


@pytest.mark.asyncio
async def test_wait_for_call_resolves_when_sync_method_is_called_in_another_thread():
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", timeout=1.0))
    await asyncio.sleep(0)

    await asyncio.to_thread(component.method)
    await waiter


@pytest.mark.asyncio
async def test_cancelling_wait_for_call_during_notification_does_not_report_event_loop_error(
):
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    loop = asyncio.get_running_loop()
    loop_errors: list[dict[str, Any]] = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: loop_errors.append(context))

    try:
        waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", timeout=1.0))
        await asyncio.sleep(0)

        component.method()
        waiter.cancel()

        with pytest.raises(asyncio.CancelledError):
            await waiter
        await asyncio.sleep(0)
    finally:
        loop.set_exception_handler(previous_handler)

    assert loop_errors == []
