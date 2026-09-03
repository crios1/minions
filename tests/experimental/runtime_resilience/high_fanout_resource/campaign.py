import asyncio
import time
from dataclasses import dataclass

import pytest

from minions import Gru
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
    PIPELINE_EVENT_FANOUT_TOTAL,
    RESOURCE_LATENCY_SECONDS,
    RESOURCE_SERVES_TOTAL,
)
from minions.implementations import NoOpLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.experimental.runtime_resilience.high_fanout_resource.components import (
    CampaignConfig,
    FanoutPipeline,
    SlowResourceMinion,
    SlowSharedResource,
)

_SCENARIOS = ((16, 1), (64, 1), (128, 1), (32, 32))
_COMPLETION_TIMEOUT_SECONDS = 10.0
_MIN_EXPECTED_CONCURRENCY_RATIO = 0.75


@dataclass(frozen=True, slots=True)
class FanoutSample:
    subscribers: int
    events: int
    workflows: int
    elapsed_seconds: float
    peak_resource_calls: int
    metric_label_sets: int


def _label_set_count(metrics: InMemoryMetrics) -> int:
    snapshot = metrics.snapshot()
    return sum(
        len(samples) for metric_kind in snapshot.values() for samples in metric_kind.values()
    )


async def _run_scenario(subscribers: int, events: int) -> FanoutSample:
    expected_workflows = subscribers * events
    current_task = asyncio.current_task()
    baseline_tasks = {
        task for task in asyncio.all_tasks() if task is not current_task and not task.done()
    }

    logger = NoOpLogger()
    metrics = InMemoryMetrics(logger=logger)
    state_store = InMemoryStateStore(logger=logger)
    SlowSharedResource.reset(expected_calls=expected_workflows)
    FanoutPipeline.reset(expected_subs=subscribers, total_events=events)

    gru = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    started_at = time.perf_counter()
    starts = await asyncio.gather(
        *(
            gru.start_orchestration(
                pipeline=FanoutPipeline,
                minion=SlowResourceMinion,
                minion_config=CampaignConfig(subscriber_index=index),
            )
            for index in range(subscribers)
        )
    )
    failures = [result for result in starts if not result.success]
    assert not failures, failures
    orchestration_ids = tuple(
        result.orchestration_id for result in starts if result.orchestration_id is not None
    )
    assert len(orchestration_ids) == subscribers
    assert len(set(orchestration_ids)) == subscribers

    completed = SlowSharedResource.all_calls_completed
    assert completed is not None
    await asyncio.wait_for(completed.wait(), timeout=_COMPLETION_TIMEOUT_SECONDS)

    deadline = asyncio.get_running_loop().time() + _COMPLETION_TIMEOUT_SECONDS
    while (
        metrics.snapshot_counter_value_total(MINION_WORKFLOW_SUCCEEDED_TOTAL) != expected_workflows
    ):
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError("Timed out waiting for workflow success metrics")
        await asyncio.sleep(0)
    elapsed_seconds = time.perf_counter() - started_at

    assert FanoutPipeline.events_produced == events
    assert SlowSharedResource.calls_started == expected_workflows
    assert SlowSharedResource.calls_completed == expected_workflows
    assert SlowSharedResource.calls_inflight == 0
    assert SlowSharedResource.peak_calls_inflight >= int(
        subscribers * _MIN_EXPECTED_CONCURRENCY_RATIO
    )
    assert metrics.snapshot_counter_value_total(PIPELINE_EVENT_FANOUT_TOTAL) == expected_workflows
    assert len(metrics.snapshot_counters()[PIPELINE_EVENT_FANOUT_TOTAL]) == subscribers
    assert (
        metrics.snapshot_counter_value_total(MINION_WORKFLOW_SUCCEEDED_TOTAL) == expected_workflows
    )
    assert len(metrics.snapshot_counters()[MINION_WORKFLOW_SUCCEEDED_TOTAL]) == subscribers
    assert metrics.snapshot_counter_value_total(RESOURCE_SERVES_TOTAL) == expected_workflows
    assert len(metrics.snapshot_counters()[RESOURCE_SERVES_TOTAL]) == subscribers
    assert metrics.snapshot_histogram_count_total(RESOURCE_LATENCY_SECONDS) == expected_workflows
    assert len(metrics.snapshot_histograms()[RESOURCE_LATENCY_SECONDS]) == subscribers
    metrics.assert_metric_label_observations_match_contract()
    assert await state_store.get_all_contexts() == []
    metric_label_sets = _label_set_count(metrics)
    assert metric_label_sets <= subscribers * 20

    for orchestration_id in reversed(orchestration_ids):
        stopped = await gru.stop_orchestration(orchestration_id)
        assert stopped.success
    assert (await gru.runtime_state_snapshot()).is_empty
    shutdown = await gru.shutdown()
    assert shutdown.success
    assert (await gru.runtime_state_snapshot()).is_empty

    await asyncio.sleep(0)
    remaining_tasks = {
        task for task in asyncio.all_tasks() if task is not current_task and not task.done()
    }
    assert remaining_tasks <= baseline_tasks

    return FanoutSample(
        subscribers=subscribers,
        events=events,
        workflows=expected_workflows,
        elapsed_seconds=elapsed_seconds,
        peak_resource_calls=SlowSharedResource.peak_calls_inflight,
        metric_label_sets=metric_label_sets,
    )


@pytest.mark.asyncio
async def test_high_fanout_shared_slow_resource_remains_correct_and_bounded():
    samples = [await _run_scenario(subscribers, events) for subscribers, events in _SCENARIOS]
    for sample in samples:
        print(
            "high_fanout_resource_sample "
            f"subscribers={sample.subscribers} "
            f"events={sample.events} "
            f"workflows={sample.workflows} "
            f"elapsed_seconds={sample.elapsed_seconds:.6f} "
            f"peak_resource_calls={sample.peak_resource_calls} "
            f"metric_label_sets={sample.metric_label_sets}"
        )

    assert [(sample.subscribers, sample.events) for sample in samples] == list(_SCENARIOS)
    assert all(sample.elapsed_seconds < _COMPLETION_TIMEOUT_SECONDS for sample in samples)
