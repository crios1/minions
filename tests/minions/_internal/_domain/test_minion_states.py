import pytest
import asyncio
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_ABORTED_TOTAL,
    MINION_WORKFLOW_STEP_FAILED_TOTAL,
    MINION_WORKFLOW_STEP_SUCCEEDED_TOTAL,
    LABEL_MINION
)

from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from minions import minion_step, Minion
from minions._internal._domain.exceptions import AbortWorkflow

from pprint import pprint

@pytest.mark.asyncio
async def test_workflow_aborted_increments_aborted_counter():
    # define a minion whose first step aborts
    class AbortMinion(Minion[dict, dict]):
        name = "abort-minion"

        @minion_step
        async def step_1(self):
            raise AbortWorkflow()

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    # start the minion by calling _handle_event directly with a dummy event
    m = AbortMinion('iid', 'ck', 'tests.assets.abort_minion', 'cfg', state_store, metrics, logger)
    await m._handle_event({})

    # wait until the state store has recorded deletion (workflow finished)
    await state_store.wait_for_call('delete_context', count=1, timeout=2)

    snap = metrics.snapshot()
    pprint(snap)
    counters = snap.get('counters', {})

    # workflow started should be incremented and aborted counter incremented
    assert counters.get(MINION_WORKFLOW_STARTED_TOTAL)
    assert sum(counters.get(MINION_WORKFLOW_ABORTED_TOTAL, {}).values()) == 1

    # no Gru instance created here


@pytest.mark.asyncio
async def test_workflow_failed_increments_failed_counter():
    class FailMinion(Minion[dict, dict]):
        name = "fail-minion"

        @minion_step
        async def step_1(self):
            raise RuntimeError('boom')

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    m = FailMinion('iid', 'ck', 'tests.assets.fail_minion', 'cfg', state_store, metrics, logger)
    await m._handle_event({})

    await state_store.wait_for_call('delete_context', count=1, timeout=2)

    snap = metrics.snapshot()
    pprint(snap)
    counters = snap.get('counters', {})

    assert counters.get(MINION_WORKFLOW_STARTED_TOTAL)
    assert sum(counters.get(MINION_WORKFLOW_FAILED_TOTAL, {}).values()) == 1

    # nothing to shutdown (no Gru instance)
