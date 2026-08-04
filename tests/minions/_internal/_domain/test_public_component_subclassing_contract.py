# pyright: reportUnusedClass=false

from typing import Any

import pytest

from minions import Minion, Pipeline, Resource
from minions._internal._domain.exceptions import UnsupportedUserCode
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.support.component_subclassing_contract import (
    assert_mn_attribute_assignment_in_user_method_is_rejected,
    assert_mn_class_attribute_assignment_in_class_body_is_rejected,
    assert_safe_create_task_override_is_rejected,
)


@pytest.fixture(
    params=[
        pytest.param(Minion[EmptyEvent, EmptyContext], id="minion"),
        pytest.param(Pipeline[EmptyEvent], id="pipeline"),
        pytest.param(Resource, id="resource"),
    ],
)
def public_component_base(request: pytest.FixtureRequest) -> Any:
    return request.param


class TestReservedMnAttributeSpace:
    def test_mn_class_attribute_assignment_in_class_body_is_rejected(
        self,
        public_component_base: Any,
    ) -> None:
        assert_mn_class_attribute_assignment_in_class_body_is_rejected(
            public_component_base
        )

    def test_mn_class_attribute_annotation_in_class_body_is_rejected(
        self,
        public_component_base: Any,
    ) -> None:
        with pytest.raises(UnsupportedUserCode):

            class InvalidUserComponent(public_component_base):
                _mn_bad_class_attribute: int

    def test_mn_method_definition_is_rejected(
        self,
        public_component_base: Any,
    ) -> None:
        with pytest.raises(UnsupportedUserCode):

            class InvalidUserComponent(public_component_base):
                async def _mn_bad_method(self) -> None:
                    pass

    def test_mn_attribute_assignment_in_user_method_is_rejected(
        self,
        public_component_base: Any,
    ) -> None:
        assert_mn_attribute_assignment_in_user_method_is_rejected(
            public_component_base
        )


class TestFinalPublicOperations:
    def test_safe_create_task_override_is_rejected(
        self,
        public_component_base: Any,
    ) -> None:
        assert_safe_create_task_override_is_rejected(public_component_base)
