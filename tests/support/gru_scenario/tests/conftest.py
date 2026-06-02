from collections.abc import Iterator

import pytest

import minions._internal._domain.component_identity as component_identity


@pytest.fixture(autouse=True)
def _isolate_component_id_registry() -> Iterator[None]:  # pyright: ignore[reportUnusedFunction]
    registry_snapshot = dict(component_identity._COMPONENT_ID_REGISTRY)
    try:
        yield
    finally:
        component_identity._COMPONENT_ID_REGISTRY.clear()
        component_identity._COMPONENT_ID_REGISTRY.update(registry_snapshot)
