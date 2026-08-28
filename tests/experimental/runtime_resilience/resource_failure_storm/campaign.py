import asyncio
from typing import Protocol, cast

import pytest

from minions import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_runtime_component_maps_consistent,
    assert_runtime_resource_maps_consistent,
)

_CYCLES = 64
_TIMEOUT_SECONDS = 2.0
_FAILING_RESOURCE_ID = "tests.assets.crash.resources.gated_boom_run.AssetResource"

_HEALTHY = (
    "tests.assets.pipelines.emit_one.counter.with_fixed_resource",
    "tests.assets.minions.two_steps.counter.with_fixed_resource",
)
_DEPENDENT_A = (
    "tests.assets.pipelines.emit_one.counter.default",
    "tests.assets.crash.minions.counter.with_resource_depending_on_gated_boom_run",
)
_DEPENDENT_B = (
    "tests.assets.pipelines.emit_one.counter.default",
    "tests.assets.crash.minions.counter.with_resource_depending_on_gated_boom_run_b",
)
_PROBE = (
    "tests.assets.pipelines.emit_one.counter.default_b",
    "tests.assets.minions.one_step.counter.default",
)


class _TriggerableFailure(Protocol):
    def trigger_run_failure(self) -> None: ...


async def _wait_until_removed(
    gru: Gru,
    orchestration_ids: frozenset[str],
) -> None:
    async def wait() -> None:
        while orchestration_ids & (await gru.runtime_state_snapshot()).orchestrations:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=_TIMEOUT_SECONDS)


async def _wait_for_finalizers(gru: Gru) -> None:
    async def wait() -> None:
        while gru._runtime_failure_finalizer_tasks:
            await asyncio.sleep(0)

    await asyncio.wait_for(wait(), timeout=_TIMEOUT_SECONDS)


def _find_failing_resource(gru: Gru) -> tuple[str, _TriggerableFailure]:
    resource = gru._resources.get(_FAILING_RESOURCE_ID)
    assert resource is not None
    assert type(resource).__module__ == ("tests.assets.crash.resources.gated_boom_run")
    return _FAILING_RESOURCE_ID, cast(_TriggerableFailure, resource)


@pytest.mark.asyncio
async def test_repeated_shared_resource_failures_remain_contained():
    logger = InMemoryLogger()
    metrics = InMemoryMetrics(logger=logger)
    state_store = InMemoryStateStore(logger=logger)
    gru = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    try:
        healthy = await gru.start_orchestration(
            pipeline=_HEALTHY[0],
            minion=_HEALTHY[1],
        )
        assert healthy.success
        assert healthy.orchestration_id is not None
        healthy_id = healthy.orchestration_id

        healthy_snapshot = await gru.runtime_state_snapshot()
        healthy_resources = healthy_snapshot.resources
        assert len(healthy_resources) == 1
        healthy_resource_id = next(iter(healthy_resources))
        assert healthy_snapshot.resource_refcount(healthy_resource_id) == 2

        for cycle in range(_CYCLES):
            dependent_a, dependent_b = await asyncio.gather(
                gru.start_orchestration(
                    pipeline=_DEPENDENT_A[0],
                    minion=_DEPENDENT_A[1],
                ),
                gru.start_orchestration(
                    pipeline=_DEPENDENT_B[0],
                    minion=_DEPENDENT_B[1],
                ),
            )
            assert dependent_a.success, f"cycle={cycle}: {dependent_a}"
            assert dependent_b.success, f"cycle={cycle}: {dependent_b}"
            assert dependent_a.orchestration_id is not None
            assert dependent_b.orchestration_id is not None
            dependent_ids = frozenset(
                (
                    dependent_a.orchestration_id,
                    dependent_b.orchestration_id,
                )
            )

            before_failure = await gru.runtime_state_snapshot()
            assert before_failure.orchestrations == dependent_ids | {healthy_id}
            assert len(before_failure.resources) == 3
            failing_resource_id, failing_resource = _find_failing_resource(gru)
            dependent_resource_ids = before_failure.dependents_for_resource(failing_resource_id)
            assert len(dependent_resource_ids) == 1
            dependent_resource_id = next(iter(dependent_resource_ids))
            assert before_failure.resource_refcount(failing_resource_id) == 1
            assert before_failure.resource_refcount(dependent_resource_id) == 2
            assert before_failure.resource_refcount(healthy_resource_id) == 2
            await assert_runtime_component_maps_consistent(gru)
            await assert_runtime_resource_maps_consistent(gru)

            failing_resource.trigger_run_failure()
            probe_task = asyncio.create_task(
                gru.start_orchestration(
                    pipeline=_PROBE[0],
                    minion=_PROBE[1],
                )
            )
            await _wait_until_removed(gru, dependent_ids)
            probe = await asyncio.wait_for(probe_task, timeout=_TIMEOUT_SECONDS)
            assert probe.success, f"cycle={cycle}: {probe}"
            assert probe.orchestration_id is not None
            await _wait_for_finalizers(gru)

            after_failure = await gru.runtime_state_snapshot()
            assert after_failure.orchestrations == {
                healthy_id,
                probe.orchestration_id,
            }
            assert after_failure.resources == healthy_resources
            assert after_failure.resource_refcount(healthy_resource_id) == 2
            assert failing_resource_id not in gru._resources
            assert dependent_resource_id not in gru._resources
            await assert_runtime_component_maps_consistent(gru)
            await assert_runtime_resource_maps_consistent(gru)

            stopped_probe = await gru.stop_orchestration(probe.orchestration_id)
            assert stopped_probe.success
            probe_stopped = await gru.runtime_state_snapshot()
            assert probe_stopped.orchestrations == {healthy_id}
            assert probe_stopped.resources == healthy_resources
            assert probe_stopped.resource_refcount(healthy_resource_id) == 2

        assert await state_store.get_all_contexts() == []
        stopped_healthy = await gru.stop_orchestration(healthy_id)
        assert stopped_healthy.success
        assert (await gru.runtime_state_snapshot()).is_empty
        shutdown = await gru.shutdown()
        assert shutdown.success
        assert (await gru.runtime_state_snapshot()).is_empty
    finally:
        await gru.shutdown()
