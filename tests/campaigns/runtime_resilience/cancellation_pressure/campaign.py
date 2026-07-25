import asyncio
from typing import Literal

import pytest

from minions import Gru
from minions._internal._framework.metrics_constants import (
    LABEL_STATUS,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
)
from minions.implementations import NoOpLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.campaigns.runtime_resilience.cancellation_pressure.components import (
    BurstPipeline,
    CampaignConfig,
    GatedResourceMinion,
    GatedSharedResource,
)

_TIMEOUT_SECONDS = 10.0


async def _wait_for_counter(
    metrics: InMemoryMetrics,
    metric_name: str,
    expected: int,
) -> None:
    async def wait() -> None:
        while metrics.snapshot_counter_value_total(metric_name) != expected:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=_TIMEOUT_SECONDS)


def _duration_count_for_status(
    metrics: InMemoryMetrics,
    metric_name: str,
    status: str,
) -> int:
    return int(
        sum(
            sample["count"]
            for sample in metrics.snapshot_histograms().get(metric_name, [])
            if sample["labels"].get(LABEL_STATUS) == status
        )
    )


async def _start_subscribers(
    gru: Gru,
    subscribers: int,
) -> tuple[str, ...]:
    results = await asyncio.gather(
        *(
            gru.start_orchestration(
                pipeline=BurstPipeline,
                minion=GatedResourceMinion,
                minion_config=CampaignConfig(subscriber_index=index),
            )
            for index in range(subscribers)
        )
    )
    failures = [result for result in results if not result.success]
    assert not failures, failures
    orchestration_ids = tuple(
        result.orchestration_id for result in results if result.orchestration_id is not None
    )
    assert len(orchestration_ids) == subscribers
    assert len(set(orchestration_ids)) == subscribers
    return orchestration_ids


async def _run_interruption_and_replay(
    *,
    action: Literal["stop", "shutdown"],
    subscribers: int,
    events: int,
) -> None:
    expected_workflows = subscribers * events
    logger = NoOpLogger()
    metrics = InMemoryMetrics(logger=logger)
    state_store = InMemoryStateStore(logger=logger)
    GatedSharedResource.reset(expected_calls=expected_workflows)
    BurstPipeline.reset(expected_subs=subscribers, total_events=events)

    gru = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    orchestration_ids = await _start_subscribers(gru, subscribers)
    all_calls_started = GatedSharedResource.all_calls_started
    assert all_calls_started is not None
    await asyncio.wait_for(
        all_calls_started.wait(),
        timeout=_TIMEOUT_SECONDS,
    )
    assert BurstPipeline.events_produced == events
    assert GatedSharedResource.calls_started == expected_workflows

    if action == "stop":
        stops = await asyncio.gather(
            *(gru.stop_orchestration(orchestration_id) for orchestration_id in orchestration_ids)
        )
        assert all(stop.success for stop in stops)
        assert (await gru.runtime_state_snapshot()).is_empty
    else:
        shutdown = await gru.shutdown()
        assert shutdown.success
        assert (await gru.runtime_state_snapshot()).is_empty

    contexts = await state_store.get_all_contexts()
    assert len(contexts) == expected_workflows
    assert len({context.workflow_id for context in contexts}) == expected_workflows
    assert {context.orchestration_id for context in contexts} == set(orchestration_ids)
    assert (
        _duration_count_for_status(
            metrics,
            MINION_WORKFLOW_DURATION_SECONDS,
            "interrupted",
        )
        == expected_workflows
    )
    assert (
        _duration_count_for_status(
            metrics,
            MINION_WORKFLOW_STEP_DURATION_SECONDS,
            "interrupted",
        )
        == expected_workflows
    )

    if action == "shutdown":
        gru = await Gru.create(
            logger=logger,
            metrics=metrics,
            state_store=state_store,
        )

    GatedSharedResource.release()
    BurstPipeline.reset(expected_subs=subscribers, total_events=0)
    restarted_ids = await _start_subscribers(gru, subscribers)
    assert restarted_ids == orchestration_ids
    await _wait_for_counter(
        metrics,
        MINION_WORKFLOW_SUCCEEDED_TOTAL,
        expected_workflows,
    )
    assert await state_store.get_all_contexts() == []
    metrics.assert_recorded_labels_match_contract()

    stops = await asyncio.gather(
        *(gru.stop_orchestration(orchestration_id) for orchestration_id in orchestration_ids)
    )
    assert all(stop.success for stop in stops)
    assert (await gru.runtime_state_snapshot()).is_empty
    shutdown = await gru.shutdown()
    assert shutdown.success
    assert (await gru.runtime_state_snapshot()).is_empty


@pytest.mark.asyncio
async def test_stop_interrupts_and_replays_high_fanout_workflows() -> None:
    await _run_interruption_and_replay(
        action="stop",
        subscribers=32,
        events=16,
    )


@pytest.mark.asyncio
async def test_shutdown_interrupts_and_replays_high_fanout_workflows() -> None:
    await _run_interruption_and_replay(
        action="shutdown",
        subscribers=16,
        events=16,
    )
