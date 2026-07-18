"""Test expectations over Gru's public runtime snapshot API."""

from collections.abc import Mapping, Set

from minions._internal._domain.gru import Gru


def assert_runtime_empty(gru: Gru) -> None:
    assert gru.runtime_state_snapshot().is_empty


def assert_runtime_component_maps_consistent(gru: Gru) -> None:
    snapshot = gru.runtime_state_snapshot()

    assert snapshot.minion_instances == snapshot.minion_tasks
    assert snapshot.pipelines == snapshot.pipeline_tasks
    assert snapshot.resources == snapshot.resource_tasks


def assert_orchestration_running(gru: Gru, orchestration_id: str) -> None:
    snapshot = gru.runtime_state_snapshot()
    minion_instance_id = snapshot.minion_instance_for_orchestration(orchestration_id)

    assert_runtime_component_maps_consistent(gru)
    assert minion_instance_id is not None
    assert orchestration_id in snapshot.orchestrations
    assert minion_instance_id in snapshot.minion_tasks


def assert_runtime_component_counts_exact(
    gru: Gru,
    *,
    minions: int | None = None,
    pipelines: int | None = None,
    resources: int | None = None,
) -> None:
    """Assert exact live component counts and task-map parity."""
    snapshot = gru.runtime_state_snapshot()

    assert_runtime_component_maps_consistent(gru)
    if minions is not None:
        assert len(snapshot.orchestrations) == minions
        assert len(snapshot.minion_instances) == minions
        assert len(snapshot.minion_tasks) == minions
    if pipelines is not None:
        assert len(snapshot.pipelines) == pipelines
        assert len(snapshot.pipeline_tasks) == pipelines
    if resources is not None:
        assert len(snapshot.resources) == resources
        assert len(snapshot.resource_tasks) == resources


def assert_runtime_component_counts_at_least(
    gru: Gru,
    *,
    minions: int | None = None,
    pipelines: int | None = None,
    resources: int | None = None,
) -> None:
    """Assert minimum live component counts and task-map parity."""
    snapshot = gru.runtime_state_snapshot()

    assert_runtime_component_maps_consistent(gru)
    if minions is not None:
        assert len(snapshot.orchestrations) >= minions
        assert len(snapshot.minion_instances) >= minions
        assert len(snapshot.minion_tasks) >= minions
    if pipelines is not None:
        assert len(snapshot.pipelines) >= pipelines
        assert len(snapshot.pipeline_tasks) >= pipelines
    if resources is not None:
        assert len(snapshot.resources) >= resources
        assert len(snapshot.resource_tasks) >= resources


def assert_running_minions(
    gru: Gru,
    *,
    orchestration_to_minion_instance: Mapping[str, str],
) -> None:
    snapshot = gru.runtime_state_snapshot()
    minion_instance_ids = set(orchestration_to_minion_instance.values())

    assert_runtime_component_maps_consistent(gru)
    assert snapshot.minion_instance_by_orchestration == dict(orchestration_to_minion_instance)
    assert snapshot.orchestrations == set(orchestration_to_minion_instance)
    assert snapshot.minion_instances == minion_instance_ids
    assert snapshot.minion_tasks == minion_instance_ids


def assert_pipeline_singleton(
    gru: Gru,
    *,
    pipeline_id: str,
    orchestration_ids: Set[str],
) -> None:
    snapshot = gru.runtime_state_snapshot()

    assert_runtime_component_maps_consistent(gru)
    assert snapshot.pipelines == {pipeline_id}
    assert snapshot.pipeline_tasks == {pipeline_id}
    assert snapshot.pipeline_by_orchestration == {
        orchestration_id: pipeline_id for orchestration_id in orchestration_ids
    }
    for orchestration_id in orchestration_ids:
        assert snapshot.pipeline_for_orchestration(orchestration_id) == pipeline_id


def assert_runtime_resource_maps_consistent(gru: Gru) -> None:
    """Assert resource association maps only reference live runtime ids."""
    snapshot = gru.runtime_state_snapshot()

    assert_runtime_component_maps_consistent(gru)
    for minion_instance_id, resource_ids in snapshot.resources_by_minion_instance.items():
        assert minion_instance_id in snapshot.minion_instances
        assert resource_ids <= snapshot.resources
    for pipeline_id, resource_ids in snapshot.resources_by_pipeline.items():
        assert pipeline_id in snapshot.pipelines
        assert resource_ids <= snapshot.resources
    for dependent_id, dependency_ids in (
        snapshot.resource_dependencies_by_dependent_resource.items()
    ):
        assert dependent_id in snapshot.resources
        assert dependency_ids <= snapshot.resources
        for dependency_id in dependency_ids:
            assert dependent_id in snapshot.dependents_for_resource(dependency_id)
    for dependency_id, dependent_ids in (
        snapshot.resource_dependents_by_dependency_resource.items()
    ):
        assert dependency_id in snapshot.resources
        assert dependent_ids <= snapshot.resources
        for dependent_id in dependent_ids:
            assert dependency_id in snapshot.dependencies_for_resource(dependent_id)


def assert_pipeline_resource_dependency_singletons(
    gru: Gru,
    *,
    pipeline_id: str,
    owner_resource_id: str,
    dependency_resource_id: str,
    owner_refcount: int,
    dependency_refcount: int,
) -> None:
    snapshot = gru.runtime_state_snapshot()

    assert_runtime_component_maps_consistent(gru)
    assert snapshot.resources == {owner_resource_id, dependency_resource_id}
    assert snapshot.resource_tasks == {owner_resource_id, dependency_resource_id}
    assert snapshot.resources_for_pipeline(pipeline_id) == {owner_resource_id}
    assert snapshot.dependencies_for_resource(owner_resource_id) == {dependency_resource_id}
    assert snapshot.dependents_for_resource(dependency_resource_id) == {owner_resource_id}
    assert snapshot.resource_refcount(owner_resource_id) == owner_refcount
    assert snapshot.resource_refcount(dependency_resource_id) == dependency_refcount
