import pytest

from minions._internal._framework.logger_noop import NoOpLogger
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext

@pytest.mark.asyncio
async def test_inmemory_state_store_crud():
    store = InMemoryStateStore(NoOpLogger())

    ctx1 = MinionWorkflowContext(
        minion_modpath="mock.minion.modpath.1",
        workflow_id="wf1",
        event=dict(),
        context=dict(),
        context_cls=dict
    )
    ctx2 = MinionWorkflowContext(
        minion_modpath="mock.minion.modpath.2",
        workflow_id="wf2",
        event=dict(),
        context=dict(),
        context_cls=dict
    )

    await store.save_context(ctx1)
    await store.save_context(ctx2)

    contexts = await store.get_all_contexts()
    workflow_ids = {ctx.workflow_id for ctx in contexts}
    assert workflow_ids == {"wf1", "wf2"}

    await store.delete_context("wf1")
    contexts = await store.get_all_contexts()
    workflow_ids = {ctx.workflow_id for ctx in contexts}
    assert workflow_ids == {"wf2"}

    await store.delete_context("wf2")
    contexts = await store.get_all_contexts()
    assert contexts == []
