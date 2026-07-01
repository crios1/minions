"""Test expectations over Gru's public runtime snapshot API."""

from collections.abc import Mapping, Set

from minions._internal._domain.gru import Gru


def assert_runtime_empty(gru: Gru) -> None:
    assert gru.runtime_state_snapshot().is_empty


def assert_running_minions(
    gru: Gru,
    *,
    orchestration_to_minion_instance: Mapping[str, str],
) -> None:
    snapshot = gru.runtime_state_snapshot()
    minion_instance_ids = set(orchestration_to_minion_instance.values())

    assert snapshot.minion_instance_by_orchestration == dict(orchestration_to_minion_instance)
    assert snapshot.orchestrations == set(orchestration_to_minion_instance)
    assert snapshot.minion_instances == minion_instance_ids
    assert snapshot.minion_tasks == minion_instance_ids


def assert_pipeline_singleton(
    gru: Gru,
    *,
    pipeline_id: str,
    minion_instance_ids: Set[str],
) -> None:
    snapshot = gru.runtime_state_snapshot()

    assert snapshot.pipelines == {pipeline_id}
    assert snapshot.pipeline_tasks == {pipeline_id}
    assert snapshot.pipeline_by_minion_instance == {
        minion_instance_id: pipeline_id
        for minion_instance_id in minion_instance_ids
    }
    for minion_instance_id in minion_instance_ids:
        assert snapshot.pipeline_for_minion(minion_instance_id) == pipeline_id


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

    assert snapshot.resources == {owner_resource_id, dependency_resource_id}
    assert snapshot.resource_tasks == {owner_resource_id, dependency_resource_id}
    assert snapshot.resources_for_pipeline(pipeline_id) == {owner_resource_id}
    assert snapshot.dependencies_for_resource(owner_resource_id) == {dependency_resource_id}
    assert snapshot.dependents_for_resource(dependency_resource_id) == {owner_resource_id}
    assert snapshot.resource_refcount(owner_resource_id) == owner_refcount
    assert snapshot.resource_refcount(dependency_resource_id) == dependency_refcount
