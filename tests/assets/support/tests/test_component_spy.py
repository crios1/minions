import asyncio
from typing import Any

import pytest

from tests.assets.support.component_spy_meta import ComponentSpyMeta


def test_requires_explicit_enablement() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        pass

    with pytest.raises(
        RuntimeError,
        match=r"Call SpiedComponent\.enable_spy\(\) before using spy controls",
    ):
        SpiedComponent.get_call_counts()

    SpiedComponent.enable_spy()

    assert SpiedComponent.get_call_counts() == {}


def test_observes_initialization() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        pass

    SpiedComponent.enable_spy()

    SpiedComponent()

    assert SpiedComponent.get_call_counts()["__init__"] == 1


def test_observes_sync_method_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def sync_method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    component.sync_method()

    assert SpiedComponent.get_call_counts() == {"sync_method": 1}


@pytest.mark.asyncio
async def test_observes_async_method_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def async_method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    await component.async_method()

    assert SpiedComponent.get_call_counts() == {"async_method": 1}


def test_observes_class_method_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        @classmethod
        def class_method(cls) -> None:
            return

    SpiedComponent.enable_spy()

    SpiedComponent.class_method()

    assert SpiedComponent.get_call_counts() == {"class_method": 1}


def test_observes_static_method_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        @staticmethod
        def static_method() -> None:
            return

    SpiedComponent.enable_spy()

    SpiedComponent.static_method()

    assert SpiedComponent.get_call_counts() == {"static_method": 1}


def test_does_not_observe_property_access() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        @property
        def value(self) -> int:
            return 1

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    assert component.value == 1

    assert SpiedComponent.get_call_counts() == {}


def test_observation_does_not_mutate_component_instance() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def __init__(self) -> None:
            self.value = "original"

        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()

    assert vars(component) == {"value": "original"}

    SpiedComponent.reset_spy()
    component.method()

    assert SpiedComponent.get_call_counts() == {"method": 1}
    assert vars(component) == {"value": "original"}


def test_assigns_distinct_identities_to_component_instances() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        pass

    SpiedComponent.enable_spy()
    first_component = SpiedComponent()
    second_component = SpiedComponent()
    first_tag = SpiedComponent.get_instance_tag(first_component)
    second_tag = SpiedComponent.get_instance_tag(second_component)

    assert first_tag is not None
    assert second_tag is not None
    assert first_tag != second_tag
    assert SpiedComponent.get_instance_tags() == {first_tag, second_tag}


def test_call_counts_and_history_are_isolated_between_component_classes() -> None:
    class FirstSpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    class SecondSpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    FirstSpiedComponent.enable_spy()
    SecondSpiedComponent.enable_spy()
    first_component = FirstSpiedComponent()
    SecondSpiedComponent()
    FirstSpiedComponent.reset_spy()
    SecondSpiedComponent.reset_spy()

    first_component.method()

    assert FirstSpiedComponent.get_call_counts() == {"method": 1}
    assert [name for name, _, _ in FirstSpiedComponent.get_call_history()] == ["method"]
    assert SecondSpiedComponent.get_call_counts() == {}
    assert SecondSpiedComponent.get_call_history() == []


def test_repeated_enablement_records_each_call_once() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    SpiedComponent.enable_spy()
    component = SpiedComponent()
    component.method()

    assert SpiedComponent.get_call_counts() == {"__init__": 1, "method": 1}
    assert [name for name, _, _ in SpiedComponent.get_call_history()] == [
        "__init__",
        "method",
    ]


def test_reset_clears_counts_and_history() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    component.method()

    SpiedComponent.reset_spy()

    assert SpiedComponent.get_call_counts() == {}
    assert SpiedComponent.get_call_history() == []


def test_repeated_reset_keeps_counts_and_history_empty() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        pass

    SpiedComponent.enable_spy()

    SpiedComponent.reset_spy()
    SpiedComponent.reset_spy()

    assert SpiedComponent.get_call_counts() == {}
    assert SpiedComponent.get_call_history() == []


def test_reset_clears_instance_tags() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    first_component = SpiedComponent()
    second_component = SpiedComponent()
    assert SpiedComponent.get_instance_tag(first_component) is not None
    assert SpiedComponent.get_instance_tag(second_component) is not None

    SpiedComponent.reset_spy()

    assert SpiedComponent.get_instance_tag(first_component) is None
    assert SpiedComponent.get_instance_tag(second_component) is None
    second_component.method()
    assert SpiedComponent.get_instance_tag(second_component) is not None


@pytest.mark.asyncio
async def test_reset_raises_while_wait_for_call_is_pending() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    waiter = asyncio.create_task(SpiedComponent.wait_for_call("method", timeout=1.0))
    await asyncio.sleep(0)

    with pytest.raises(RuntimeError, match="call-count synchronization is active"):
        SpiedComponent.reset_spy()

    component.method()
    await waiter


def test_call_history_includes_chronological_timestamps() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def first(self) -> None:
            return

        def second(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    component.first()
    component.second()

    history = SpiedComponent.get_call_history()

    relevant_history = [entry for entry in history if entry[0] in {"__init__", "first", "second"}]
    assert [name for name, _, _ in relevant_history] == [
        "__init__",
        "first",
        "second",
    ]
    assert all(
        first_timestamp <= second_timestamp
        for (_, first_timestamp, _), (_, second_timestamp, _) in zip(
            history,
            history[1:],
        )
    )


def test_class_call_order_accepts_subsequence_and_reports_missing_tail() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def first(self) -> None:
            return

        def second(self) -> None:
            return

        def third(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    component.first()
    component.second()
    component.third()

    SpiedComponent.assert_call_order(sub_seq=["first", "third"])

    with pytest.raises(AssertionError) as exc_info:
        SpiedComponent.assert_call_order(["first", "missing", "third"])

    message = str(exc_info.value)
    assert "Missing from this point: ['missing', 'third']" in message
    assert "Full history names: ['first', 'second', 'third']" in message


def test_call_order_for_instance_excludes_other_instances_calls() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def first(self) -> None:
            return

        def second(self) -> None:
            return

    SpiedComponent.enable_spy()
    first_component = SpiedComponent()
    second_component = SpiedComponent()
    SpiedComponent.reset_spy()
    first_component.first()
    second_component.second()
    first_tag = SpiedComponent.get_instance_tag(first_component)
    second_tag = SpiedComponent.get_instance_tag(second_component)

    assert first_tag is not None
    assert second_tag is not None
    SpiedComponent.assert_call_order_for_instance(first_tag, ["first"])
    SpiedComponent.assert_call_order_for_instance(second_tag, ["second"])
    with pytest.raises(AssertionError, match="not found for instance tag"):
        SpiedComponent.assert_call_order_for_instance(
            first_tag,
            sub_seq=["first", "second"],
        )


@pytest.mark.asyncio
async def test_wait_resolves_immediately_when_requested_call_count_was_already_reached() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        async def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    await component.method()

    await SpiedComponent.wait_for_call("method", count=1, timeout=0)


@pytest.mark.asyncio
async def test_multiple_waiters_resolve_at_their_requested_call_counts() -> None:
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
async def test_waits_for_configured_call_counts() -> None:
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
async def test_wait_resolves_when_async_method_call_starts_not_when_it_finishes() -> None:
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
async def test_cancelled_waiter_does_not_disrupt_later_observation() -> None:
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
async def test_observes_sync_method_call_from_another_thread() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()

    await asyncio.to_thread(component.method)

    assert SpiedComponent.get_call_counts() == {"method": 1}


@pytest.mark.asyncio
async def test_wait_resolves_when_sync_method_is_called_in_another_thread() -> None:
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
async def test_cancelling_wait_during_call_notification_does_not_produce_callback_error() -> None:
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


@pytest.mark.asyncio
async def test_exact_call_limit_rejects_extra_calls_until_released() -> None:
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
async def test_reset_raises_while_call_count_limits_are_active() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    SpiedComponent.reset_spy()
    unpin_call_counts = await SpiedComponent.await_and_pin_call_counts({})

    with pytest.raises(RuntimeError, match="call-count synchronization is active"):
        SpiedComponent.reset_spy()

    with pytest.raises(AssertionError, match="unexpected call method"):
        component.method()

    unpin_call_counts()
    SpiedComponent.reset_spy()


@pytest.mark.asyncio
async def test_await_and_pin_call_counts_raises_while_limits_are_active() -> None:
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
async def test_exact_call_limit_rejects_listed_count_already_over_expected() -> None:
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
async def test_exact_call_limits_reject_unlisted_calls_by_default() -> None:
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
async def test_exact_call_limits_do_not_reject_unlisted_calls_made_before_pinning() -> None:
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
async def test_exact_call_limits_allow_unlisted_calls_when_configured() -> None:
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
async def test_limit_exceeded_callback_receives_listed_call_overflow() -> None:
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
async def test_limit_exceeded_callback_errors_propagate_for_listed_and_unlisted_calls() -> None:
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
async def test_cancelling_while_awaiting_pinned_call_counts_unpins_call_counts() -> None:
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
