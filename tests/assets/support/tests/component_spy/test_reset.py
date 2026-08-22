import asyncio

import pytest

from tests.assets.support.component_spy_meta import ComponentSpyMeta


def test_reset_clears_call_counts_and_history() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    component.method()

    SpiedComponent.reset_spy()

    assert SpiedComponent.get_call_counts() == {}
    assert SpiedComponent.get_call_history() == ()

def test_repeated_reset_keeps_call_counts_and_history_empty() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        pass

    SpiedComponent.enable_spy()

    SpiedComponent.reset_spy()
    SpiedComponent.reset_spy()

    assert SpiedComponent.get_call_counts() == {}
    assert SpiedComponent.get_call_history() == ()

def test_reset_clears_instance_identities() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    first_component = SpiedComponent()
    second_component = SpiedComponent()
    assert SpiedComponent.get_instance_identity(first_component) is not None
    assert SpiedComponent.get_instance_identity(second_component) is not None

    SpiedComponent.reset_spy()

    assert SpiedComponent.get_instance_identity(first_component) is None
    assert SpiedComponent.get_instance_identity(second_component) is None
    second_component.method()
    assert SpiedComponent.get_instance_identity(second_component) is not None


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


