from typing import Any

import pytest

from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource
from tests.support.component_subclassing_contract import (
    assert_mn_attribute_assignment_in_minion_step_is_rejected,
    assert_mn_attribute_assignment_in_user_method_is_rejected,
    assert_mn_class_attribute_assignment_in_class_body_is_rejected,
    assert_safe_create_task_override_is_rejected,
)


@pytest.fixture(
    params=[
        pytest.param(SpiedMinion[EmptyEvent, EmptyContext], id="minion"),
        pytest.param(SpiedPipeline[EmptyEvent], id="pipeline"),
        pytest.param(SpiedResource, id="resource"),
    ],
)
def spied_component_base(request: pytest.FixtureRequest) -> Any:
    return request.param


def test_mn_class_attribute_assignment_in_class_body_is_rejected(
    spied_component_base: Any,
) -> None:
    assert_mn_class_attribute_assignment_in_class_body_is_rejected(
        spied_component_base
    )


def test_mn_attribute_assignment_in_user_method_is_rejected(
    spied_component_base: Any,
) -> None:
    assert_mn_attribute_assignment_in_user_method_is_rejected(spied_component_base)


def test_safe_create_task_override_is_rejected(
    spied_component_base: Any,
) -> None:
    assert_safe_create_task_override_is_rejected(spied_component_base)


def test_mn_attribute_assignment_in_minion_step_is_rejected() -> None:
    assert_mn_attribute_assignment_in_minion_step_is_rejected(
        SpiedMinion[EmptyEvent, EmptyContext]
    )
