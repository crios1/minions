import asyncio
import importlib
from collections import Counter

import pytest

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

@pytest.mark.asyncio
async def test_gru_does_not_replay_same_workflow_id_during_startup(gru_factory):
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)
    duplicate_workflow_replay = importlib.import_module(
        "tests.assets.minions.race_cases.duplicate_workflow_replay"
    )
    duplicate_workflow_replay_minion = duplicate_workflow_replay.DuplicateWorkflowReplayMinion
    duplicate_workflow_replay_minion.reset_gates()

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_minion(
            minion="tests.assets.minions.race_cases.duplicate_workflow_replay",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        )
        assert result.success
        assert duplicate_workflow_replay_minion._gate("_startup_entered").is_set()

        def _get_step_1_workflow_ids() -> list[str]:
            return [
                log.kwargs["workflow_id"]
                for log in logger.logs
                if log.msg == "Workflow Step started"
                and log.kwargs.get("minion_name") == "startup-replay-race-minion"
                and log.kwargs.get("step_name") == "step_1"
            ]

        try:
            await asyncio.wait_for(
                duplicate_workflow_replay_minion._gate("_step_1_started").wait(),
                timeout=1.0,
            )
            await asyncio.sleep(0)

            step_1_workflow_ids = _get_step_1_workflow_ids()
            assert step_1_workflow_ids, (
                "Expected startup-replay-race-minion to start step_1."
            )

            counts = Counter(step_1_workflow_ids)
            duplicate_step_1_workflow_ids = sorted(
                workflow_id
                for workflow_id, count in counts.items()
                if count > 1
            )

            assert not duplicate_step_1_workflow_ids, (
                "A single workflow_id started step_1 more than once during startup: "
                f"{duplicate_step_1_workflow_ids}"
            )
        finally:
            duplicate_workflow_replay_minion._gate("_allow_step_1_finish").set()
