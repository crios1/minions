from typing import Any

import pytest

from minions import Minion
from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.resource import Resource
from minions._internal._framework.logger import Logger
from minions._internal._framework.metrics import Metrics
from minions._internal._framework.state_store import StateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.component_spy_meta import ComponentSpyMeta
from tests.assets.support.logger_spied import SpiedLogger
from tests.assets.support.metrics_spied import SpiedMetrics
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource
from tests.assets.support.state_store_spied import SpiedStateStore
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
        pytest.param(SpiedLogger, id="logger"),
        pytest.param(SpiedMetrics, id="metrics"),
        pytest.param(SpiedStateStore, id="state-store"),
    ],
)
def spied_component_base(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(
    params=[
        pytest.param(SpiedMinion[EmptyEvent, EmptyContext], id="minion"),
        pytest.param(SpiedPipeline[EmptyEvent], id="pipeline"),
        pytest.param(SpiedResource, id="resource"),
    ],
)
def spied_async_service_base(request: pytest.FixtureRequest) -> Any:
    return request.param


@pytest.fixture(
    params=[
        pytest.param((SpiedMinion, Minion), id="minion"),
        pytest.param((SpiedPipeline, Pipeline), id="pipeline"),
        pytest.param((SpiedResource, Resource), id="resource"),
        pytest.param((SpiedLogger, Logger), id="logger"),
        pytest.param((SpiedMetrics, Metrics), id="metrics"),
        pytest.param((SpiedStateStore, StateStore), id="state-store"),
    ],
)
def spied_base_and_component_base(
    request: pytest.FixtureRequest,
) -> tuple[type[object], type[object]]:
    return request.param


def test_spied_base_directly_inherits_component_base(
    spied_base_and_component_base: tuple[type[object], type[object]],
) -> None:
    spied_base, component_base = spied_base_and_component_base
    assert spied_base.__bases__ == (component_base,)


def test_spied_base_uses_component_spy_metaclass(
    spied_base_and_component_base: tuple[type[object], type[object]],
) -> None:
    spied_base, _ = spied_base_and_component_base
    assert isinstance(spied_base, ComponentSpyMeta)


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


def test_mn_attribute_assignment_in_minion_step_is_rejected() -> None:
    assert_mn_attribute_assignment_in_minion_step_is_rejected(
        SpiedMinion[EmptyEvent, EmptyContext]
    )


def test_safe_create_task_override_is_rejected(
    spied_async_service_base: Any,
) -> None:
    assert_safe_create_task_override_is_rejected(spied_async_service_base)
