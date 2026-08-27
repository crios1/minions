# pyright: reportUnusedClass=false

from typing import Any

import pytest

from minions import Minion, Pipeline, Resource
from minions._internal._domain.exceptions import UnsupportedUserCode
from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics
from minions._internal._framework.state_store import StateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.support.component_subclassing_contract import (
    assert_mn_attribute_annotation_with_value_is_rejected,
    assert_mn_attribute_annotation_without_value_is_rejected,
    assert_mn_attribute_assignment_in_call_is_rejected,
    assert_mn_attribute_assignment_in_init_is_rejected,
    assert_mn_attribute_assignment_in_private_method_is_rejected,
    assert_mn_attribute_assignment_in_property_deleter_is_rejected,
    assert_mn_attribute_assignment_in_property_getter_is_rejected,
    assert_mn_attribute_assignment_in_property_setter_is_rejected,
    assert_mn_attribute_assignment_in_user_method_is_rejected,
    assert_mn_attribute_assignment_through_cls_is_rejected,
    assert_mn_attribute_assignment_through_self_class_is_rejected,
    assert_mn_attribute_assignment_through_type_self_is_rejected,
    assert_mn_attribute_setattr_through_cls_is_rejected,
    assert_mn_attribute_setattr_through_self_class_is_rejected,
    assert_mn_attribute_setattr_through_type_self_is_rejected,
    assert_mn_attribute_writes_to_unrelated_object_are_allowed,
    assert_mn_class_attribute_assignment_in_class_body_is_rejected,
    assert_safe_create_task_override_is_rejected,
)


@pytest.fixture(
    params=[
        pytest.param(Minion[EmptyEvent, EmptyContext], id="minion"),
        pytest.param(Pipeline[EmptyEvent], id="pipeline"),
        pytest.param(Resource, id="resource"),
        pytest.param(Logger, id="logger"),
        pytest.param(Metrics, id="metrics"),
        pytest.param(StateStore, id="state-store"),
    ],
)
def user_facing_component_base(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(
    params=[
        pytest.param(Resource, id="resource"),
        pytest.param(Logger, id="logger"),
        pytest.param(Metrics, id="metrics"),
        pytest.param(StateStore, id="state-store"),
    ],
)
def user_facing_component_base_supporting_subclass_chains(
    request: pytest.FixtureRequest,
) -> Any:
    return request.param


@pytest.fixture(
    params=[
        pytest.param(Minion[EmptyEvent, EmptyContext], id="minion"),
        pytest.param(Pipeline[EmptyEvent], id="pipeline"),
        pytest.param(Resource, id="resource"),
    ],
)
def user_facing_async_service_base(request: pytest.FixtureRequest) -> Any:
    return request.param


class TestInheritance:
    @pytest.mark.parametrize(
        "component_base_is_first",
        [True, False],
        ids=["component-base-first", "subclass-hook-base-first"],
    )
    def test_rejects_multiple_inheritance_before_init_subclass_hooks_run(
        self,
        user_facing_component_base: Any,
        component_base_is_first: bool,
    ) -> None:
        class BaseWithSubclassHook:
            def __init_subclass__(cls, **kwargs: object) -> None:
                raise AssertionError(
                    "__init_subclass__ ran before multiple inheritance was rejected"
                )

        direct_bases = (
            (user_facing_component_base, BaseWithSubclassHook)
            if component_base_is_first
            else (BaseWithSubclassHook, user_facing_component_base)
        )
        with pytest.raises(
            UnsupportedUserCode,
            match=(
                "InvalidUserComponent cannot use multiple inheritance.*"
                "Minions components must inherit from exactly one base"
            ),
        ):
            class InvalidUserComponent(
                *direct_bases  # pyright: ignore[reportUntypedBaseClass]
            ):
                pass

    def test_rejects_multiple_inheritance_despite_internal_module_name_override(
        self,
        user_facing_component_base: Any,
    ) -> None:
        class AdditionalBase:
            pass

        with pytest.raises(
            UnsupportedUserCode,
            match="InvalidUserComponent cannot use multiple inheritance",
        ):
            class InvalidUserComponent(user_facing_component_base, AdditionalBase):
                __module__ = "minions._internal.user_component"

    def test_allows_user_component_subclass_chain(
        self,
        user_facing_component_base_supporting_subclass_chains: Any,
    ) -> None:
        class UserComponent(user_facing_component_base_supporting_subclass_chains):
            pass

        class UserComponentSubclass(UserComponent):
            pass

        assert UserComponent.__bases__ == (
            user_facing_component_base_supporting_subclass_chains,
        )
        assert UserComponentSubclass.__bases__ == (UserComponent,)


class TestReservedMnAttributeSpace:
    def test_mn_class_attribute_assignment_in_class_body_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_class_attribute_assignment_in_class_body_is_rejected(
            user_facing_component_base
        )

    def test_mn_class_attribute_annotation_in_class_body_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        with pytest.raises(UnsupportedUserCode):

            class InvalidUserComponent(user_facing_component_base):
                _mn_bad_class_attribute: int

    def test_mn_method_definition_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        with pytest.raises(UnsupportedUserCode):

            class InvalidUserComponent(user_facing_component_base):
                async def _mn_bad_method(self) -> None:
                    pass

    def test_mn_attribute_assignment_in_user_method_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_user_method_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_in_private_method_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_private_method_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_in_init_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_init_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_in_call_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_call_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_through_cls_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_through_cls_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_through_type_self_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_through_type_self_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_through_self_class_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_through_self_class_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_setattr_through_cls_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_setattr_through_cls_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_setattr_through_type_self_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_setattr_through_type_self_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_setattr_through_self_class_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_setattr_through_self_class_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_annotation_with_value_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_annotation_with_value_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_annotation_without_value_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_annotation_without_value_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_writes_to_unrelated_object_are_allowed(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_writes_to_unrelated_object_are_allowed(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_in_property_getter_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_property_getter_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_in_property_setter_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_property_setter_is_rejected(
            user_facing_component_base
        )

    def test_mn_attribute_assignment_in_property_deleter_is_rejected(
        self,
        user_facing_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_property_deleter_is_rejected(
            user_facing_component_base
        )


class TestFinalPublicOperations:
    def test_safe_create_task_override_is_rejected(
        self,
        user_facing_async_service_base: Any,
    ) -> None:
        assert_safe_create_task_override_is_rejected(user_facing_async_service_base)
