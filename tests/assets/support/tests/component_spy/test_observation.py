from dataclasses import FrozenInstanceError

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

def test_assigns_distinct_spy_instance_identities() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        pass

    SpiedComponent.enable_spy()
    first_component = SpiedComponent()
    second_component = SpiedComponent()
    first_identity = SpiedComponent.get_spy_instance_identity(first_component)
    second_identity = SpiedComponent.get_spy_instance_identity(second_component)

    assert first_identity is not None
    assert second_identity is not None
    assert first_identity != second_identity
    assert SpiedComponent.get_spy_instance_identities() == {
        first_identity,
        second_identity,
    }

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
    assert [
        recorded_call.method_name
        for recorded_call in FirstSpiedComponent.get_call_history()
    ] == ["method"]
    assert SecondSpiedComponent.get_call_counts() == {}
    assert SecondSpiedComponent.get_call_history() == ()

def test_repeated_enablement_records_each_call_once() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def method(self) -> None:
            return

    SpiedComponent.enable_spy()
    SpiedComponent.enable_spy()
    component = SpiedComponent()
    component.method()

    assert SpiedComponent.get_call_counts() == {"__init__": 1, "method": 1}
    assert [
        recorded_call.method_name
        for recorded_call in SpiedComponent.get_call_history()
    ] == [
        "__init__",
        "method",
    ]

def test_call_history_is_an_immutable_chronological_snapshot() -> None:
    class SpiedComponent(metaclass=ComponentSpyMeta):
        def first(self) -> None:
            return

        def second(self) -> None:
            return

    SpiedComponent.enable_spy()
    component = SpiedComponent()
    component.first()

    history = SpiedComponent.get_call_history()
    component.second()

    assert isinstance(history, tuple)
    assert [recorded_call.method_name for recorded_call in history] == [
        "__init__",
        "first",
    ]
    assert [
        recorded_call.method_name
        for recorded_call in SpiedComponent.get_call_history()
    ] == [
        "__init__",
        "first",
        "second",
    ]
    with pytest.raises(FrozenInstanceError):
        setattr(history[0], "method_name", "changed")

def test_assert_call_order_accepts_subsequence_and_reports_missing_tail() -> None:
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

    SpiedComponent.assert_call_order(subsequence=["first", "third"])

    with pytest.raises(AssertionError) as exc_info:
        SpiedComponent.assert_call_order(["first", "missing", "third"])

    message = str(exc_info.value)
    assert "Missing from this point: ['missing', 'third']" in message
    assert "Full history names: ['first', 'second', 'third']" in message

def test_assert_call_order_for_instance_excludes_other_instances_calls() -> None:
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
    first_identity = SpiedComponent.get_spy_instance_identity(first_component)
    second_identity = SpiedComponent.get_spy_instance_identity(second_component)

    assert first_identity is not None
    assert second_identity is not None
    SpiedComponent.assert_call_order_for_instance(first_identity, ["first"])
    SpiedComponent.assert_call_order_for_instance(second_identity, ["second"])
    with pytest.raises(AssertionError, match="not found for spy instance identity"):
        SpiedComponent.assert_call_order_for_instance(
            first_identity,
            subsequence=["first", "second"],
        )


