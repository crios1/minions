from collections.abc import Iterator

import pytest

import minions._internal._domain.component_identity as component_identity
from minions import Pipeline, Resource, minion_id, pipeline_id, resource_id
from minions._internal._domain.component_identity import (
    generate_component_id,
    validate_component_id,
)

MINION_COMPONENT_ID = "11111111-1111-4111-8111-11111111111a"
PIPELINE_COMPONENT_ID = "22222222-2222-4222-8222-22222222222b"
RESOURCE_COMPONENT_ID = "33333333-3333-4333-8333-33333333333c"


@pytest.fixture(autouse=True)
def _isolate_component_id_registry() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    registry_snapshot = dict(component_identity._COMPONENT_ID_REGISTRY)
    try:
        yield
    finally:
        component_identity._COMPONENT_ID_REGISTRY.clear()
        component_identity._COMPONENT_ID_REGISTRY.update(registry_snapshot)


def test_generated_component_id_satisfies_validation_contract() -> None:
    generated_id = generate_component_id()

    assert validate_component_id(generated_id) == generated_id


def test_component_id_decorators_validate_component_kind() -> None:
    with pytest.raises(TypeError, match="@minion_id"):
        minion_id(MINION_COMPONENT_ID)(Resource)

    with pytest.raises(TypeError, match="@pipeline_id"):
        pipeline_id(PIPELINE_COMPONENT_ID)(Resource)

    with pytest.raises(TypeError, match="@resource_id"):
        resource_id(RESOURCE_COMPONENT_ID)(Pipeline)


def test_component_id_decorators_require_uuid_ids() -> None:
    with pytest.raises(ValueError, match="component id"):
        resource_id("test.resource.alpha")(Resource)

    with pytest.raises(ValueError, match="canonical lowercase UUID"):
        resource_id(RESOURCE_COMPONENT_ID.upper())(Resource)


def test_component_id_decorators_reject_duplicate_loaded_ids() -> None:
    @resource_id(RESOURCE_COMPONENT_ID)
    class FirstDuplicateResource(Resource):  # pyright: ignore[reportUnusedClass]
        pass

    with pytest.raises(ValueError, match="Duplicate resource component id"):
        @resource_id(RESOURCE_COMPONENT_ID)
        class SecondDuplicateResource(Resource):  # pyright: ignore[reportUnusedClass]
            pass
