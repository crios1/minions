import asyncio

import pytest

from tests.assets.support.component_spy_meta import ComponentSpyMeta


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_rejects_extra_calls_until_released() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    pin_call_counts_task = asyncio.create_task(
        SpiedComponent.await_and_pin_call_counts({"method": 1}, timeout=1.0)
    )
    await asyncio.sleep(0)

    await component.method()
    unpin_call_counts = await pin_call_counts_task

    with pytest.raises(AssertionError, match="call overflow for method"):
        await component.method()

    unpin_call_counts()
    await component.method()


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_rejects_overlapping_call_count_limits() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    release_call_count_limits = await SpiedComponent.await_and_pin_call_counts({})

    with pytest.raises(RuntimeError, match="call-count limits are already active"):
        await SpiedComponent.await_and_pin_call_counts({})

    with pytest.raises(AssertionError, match="unexpected call method"):
        component.method()

    release_call_count_limits()


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_rejects_requested_limit_below_current_count() -> None:
    limit_violations: list[tuple[str, int, int]] = []

    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    await component.method()
    await component.method()

    with pytest.raises(AssertionError, match="call overflow for method: 2 > 1"):
        await SpiedComponent.await_and_pin_call_counts(
            {"method": 1},
            on_limit_exceeded=lambda method_name, observed_count, allowed_count: (
                limit_violations.append((method_name, observed_count, allowed_count))
            ),
        )

    assert limit_violations == [("method", 2, 1)]
    await component.method()


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_rejects_unlisted_calls_by_default() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def listed(self) -> None:
            return

        async def unlisted(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    pin_call_counts_task = asyncio.create_task(
        SpiedComponent.await_and_pin_call_counts({"listed": 1}, timeout=1.0)
    )
    await asyncio.sleep(0)

    await component.listed()
    unpin_call_counts = await pin_call_counts_task

    with pytest.raises(AssertionError, match="unexpected call unlisted"):
        await component.unlisted()

    unpin_call_counts()


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_does_not_reject_earlier_unlisted_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def listed(self) -> None:
            return

        async def unlisted(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    await component.unlisted()
    await component.listed()

    unpin_call_counts = await SpiedComponent.await_and_pin_call_counts({"listed": 1})

    unpin_call_counts()


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_allows_unlisted_calls_when_configured() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def listed(self) -> None:
            return

        async def unlisted(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    pin_call_counts_task = asyncio.create_task(
        SpiedComponent.await_and_pin_call_counts(
            {"listed": 1},
            timeout=1.0,
            allow_unlisted=True,
        )
    )
    await asyncio.sleep(0)

    await component.unlisted()
    await component.listed()
    unpin_call_counts = await pin_call_counts_task

    unpin_call_counts()


@pytest.mark.asyncio
async def test_on_limit_exceeded_receives_listed_call_overflow() -> None:
    limit_violations: list[tuple[str, int, int]] = []

    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def listed(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    pin_call_counts_task = asyncio.create_task(
        SpiedComponent.await_and_pin_call_counts(
            {"listed": 1},
            timeout=1.0,
            on_limit_exceeded=lambda method_name, observed_count, allowed_count: (
                limit_violations.append((method_name, observed_count, allowed_count))
            ),
        )
    )
    await asyncio.sleep(0)

    await component.listed()
    unpin_call_counts = await pin_call_counts_task

    with pytest.raises(AssertionError, match="call overflow for listed"):
        await component.listed()
    assert limit_violations == [("listed", 2, 1)]

    unpin_call_counts()


@pytest.mark.asyncio
async def test_on_limit_exceeded_errors_propagate_for_listed_and_unlisted_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def listed(self) -> None:
            return

        async def unlisted(self) -> None:
            return

    def raise_extra_call_error(*_: object) -> None:
        raise RuntimeError("extra call")

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    pin_call_counts_task = asyncio.create_task(
        SpiedComponent.await_and_pin_call_counts(
            {"listed": 1},
            timeout=1.0,
            on_limit_exceeded=raise_extra_call_error,
        )
    )
    await asyncio.sleep(0)

    await component.listed()
    unpin_call_counts = await pin_call_counts_task

    with pytest.raises(RuntimeError, match="extra call"):
        await component.listed()
    with pytest.raises(RuntimeError, match="extra call"):
        await component.unlisted()

    unpin_call_counts()


@pytest.mark.asyncio
async def test_cancelling_await_and_pin_call_counts_clears_installed_limits() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def listed(self) -> None:
            return

        async def unlisted(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    pin_call_counts_task = asyncio.create_task(
        SpiedComponent.await_and_pin_call_counts(
            {"listed": 1},
            timeout=1.0,
        )
    )
    await asyncio.sleep(0)

    pin_call_counts_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pin_call_counts_task

    await component.unlisted()
