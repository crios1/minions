"""Shared assertions for production and test component subclassing contracts."""

# pyright: reportUnusedClass=false

from typing import Any

import pytest

from minions import minion_step
from minions._internal._domain.exceptions import UnsupportedUserCode


def assert_mn_class_attribute_assignment_in_class_body_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            _mn_bad_class_attribute = 1


def assert_mn_attribute_assignment_in_user_method_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                self._mn_bad_attribute = 1


def assert_mn_attribute_assignment_in_private_method_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def _operation(self) -> None:
                self._mn_bad_attribute = 1


def assert_mn_attribute_assignment_through_cls_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            @classmethod
            async def operation(cls) -> None:
                cls._mn_bad_attribute = 1


def assert_mn_attribute_assignment_through_type_self_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                type(self)._mn_bad_attribute = 1


def assert_mn_attribute_assignment_through_self_class_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                self.__class__._mn_bad_attribute = 1


def assert_mn_attribute_setattr_through_cls_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            @classmethod
            async def operation(cls) -> None:
                setattr(cls, "_mn_bad_attribute", 1)


def assert_mn_attribute_setattr_through_type_self_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                setattr(type(self), "_mn_bad_attribute", 1)


def assert_mn_attribute_setattr_through_self_class_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                setattr(self.__class__, "_mn_bad_attribute", 1)


def assert_mn_attribute_annotation_with_value_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                self._mn_bad_attribute: int = 1


def assert_mn_attribute_annotation_without_value_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            async def operation(self) -> None:
                self._mn_annotation_only: int


def assert_mn_attribute_writes_to_unrelated_object_are_allowed(
    component_base: Any,
) -> None:
    class ValidComponent(component_base):
        async def operation(self, other: object) -> None:
            other._mn_attribute = 1  # pyright: ignore[reportAttributeAccessIssue]
            setattr(other, "_mn_other_attribute", 1)


def assert_mn_attribute_assignment_in_property_getter_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            @property
            def value(self) -> int:
                self._mn_bad_attribute = 1
                return 1


def assert_mn_attribute_assignment_in_property_setter_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            @property
            def value(self) -> int:
                return 1

            @value.setter
            def value(self, value: int) -> None:
                self._mn_bad_attribute = value


def assert_mn_attribute_assignment_in_property_deleter_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidComponent(component_base):
            @property
            def value(self) -> int:
                return 1

            @value.deleter
            def value(self) -> None:
                self._mn_bad_attribute = 0


def assert_safe_create_task_override_is_rejected(
    component_base: Any,
) -> None:
    with pytest.raises(
        UnsupportedUserCode,
        match=r"`InvalidComponent\.safe_create_task`.*cannot be overridden",
    ):

        class InvalidComponent(component_base):
            def safe_create_task(self) -> None:
                pass


def assert_mn_attribute_assignment_in_minion_step_is_rejected(
    minion_base: Any,
) -> None:
    with pytest.raises(UnsupportedUserCode):

        class InvalidMinion(minion_base):
            @minion_step
            async def step(self) -> None:
                self._mn_bad_attribute = 1
