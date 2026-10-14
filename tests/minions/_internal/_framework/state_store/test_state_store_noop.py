import pytest

from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore


@pytest.mark.asyncio
async def test_is_no_op():
    store = NoOpStateStore(metrics=NoOpMetrics())
    workflow_id = "workflow-1"
    orchestration_id = "orchestration-1"

    await store.save_context(workflow_id, orchestration_id, b"serialized-context")

    assert await store.get_contexts_for_orchestration(orchestration_id) == []
    assert await store.get_all_contexts() == []

    await store.delete_context(workflow_id)

    assert await store.get_contexts_for_orchestration(orchestration_id) == []
    assert await store.get_all_contexts() == []
