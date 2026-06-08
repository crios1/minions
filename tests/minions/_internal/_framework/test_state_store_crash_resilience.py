from typing import Any

import pytest

from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.state_store import PersistenceOperationResult
from tests.assets.crash.boom import BOOM_MESSAGE, BoomError
from tests.assets.crash.support.state_store_boom_delete_context import BoomDeleteStateStore
from tests.assets.crash.support.state_store_boom_get_all_contexts import (
    BoomGetAllContextsStateStore,
)
from tests.assets.crash.support.state_store_boom_get_contexts_for_orchestration import (
    BoomGetContextsForOrchestrationStateStore,
)
from tests.assets.crash.support.state_store_boom_save_context import BoomSaveStateStore
from tests.assets.events.counter import CounterEvent
from tests.assets.support.logger_inmemory import InMemoryLogger


def make_context() -> MinionWorkflowContext[CounterEvent, dict[str, Any]]:
    return MinionWorkflowContext(
        orchestration_id="minion|config|pipeline",
        workflow_id="wf-boom",
        event=CounterEvent(seq=1),
        context={},
        context_cls=dict,
    )


@pytest.mark.asyncio
async def test_state_store_save_failure_returns_structured_result_and_logs():
    logger = InMemoryLogger()
    store = BoomSaveStateStore(logger=logger)

    result = await store._mn_serialize_and_save_context(make_context())

    assert result == PersistenceOperationResult(
        persisted=False,
        failure_stage="save",
        error=result.error,
        retryable=True,
    )
    assert type(result.error).__name__ == "BoomError"
    assert logger.has_log("BoomSaveStateStore.save_context failed")


@pytest.mark.asyncio
async def test_state_store_delete_failure_returns_structured_result_and_logs():
    logger = InMemoryLogger()
    store = BoomDeleteStateStore(logger=logger)

    result = await store._mn_delete_context("wf-boom")

    assert result == PersistenceOperationResult(
        persisted=False,
        failure_stage="delete",
        error=result.error,
        retryable=True,
    )
    assert type(result.error).__name__ == "BoomError"
    assert logger.has_log("BoomDeleteStateStore.delete_context failed")


@pytest.mark.asyncio
async def test_state_store_get_contexts_for_orchestration_failure_raises_and_logs():
    logger = InMemoryLogger()
    store = BoomGetContextsForOrchestrationStateStore(logger=logger)

    with pytest.raises(BoomError, match=BOOM_MESSAGE):
        await store._mn_get_contexts_for_orchestration("orch")
    assert logger.has_log(
        "BoomGetContextsForOrchestrationStateStore.get_contexts_for_orchestration failed"
    )


@pytest.mark.asyncio
async def test_state_store_get_all_contexts_failure_raises_and_logs():
    logger = InMemoryLogger()
    store = BoomGetAllContextsStateStore(logger=logger)

    with pytest.raises(BoomError, match=BOOM_MESSAGE):
        await store._mn_get_all_contexts()
    assert logger.has_log("BoomGetAllContextsStateStore.get_all_contexts failed")
