import asyncio
import contextlib
from collections.abc import Callable
from typing import Any

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_orchestration_running,
    assert_runtime_component_counts_exact,
    assert_runtime_empty,
)

HEALTHY_MINION = "tests.assets.minions.one_step.counter.default"
HEALTHY_PIPELINE = "tests.assets.pipelines.emit_one.counter.default"
FIXED_RESOURCE_ID = "tests.assets.resources.fixed.default.AssetResource"


@pytest.mark.asyncio
async def test_shutdown_failure_removes_target_without_affecting_other_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    shared_pipeline = "tests.assets.pipelines.emit_one.counter.with_fixed_resource"

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        healthy_start = await gru.start_orchestration(shared_pipeline, HEALTHY_MINION)
        assert healthy_start.success
        assert healthy_start.orchestration_id is not None
        healthy_runtime_state = await gru.runtime_state_snapshot()

        fail_on_shutdown_start = await gru.start_orchestration(
            shared_pipeline,
            "tests.assets.crash.minions.counter.boom_shutdown",
        )
        assert fail_on_shutdown_start.success
        assert fail_on_shutdown_start.orchestration_id is not None

        failed_stop = await gru.stop_orchestration(fail_on_shutdown_start.orchestration_id)

        assert not failed_stop.success
        assert failed_stop.reason == (
            "tests.assets.crash.minions.counter.boom_shutdown."
            "AssetMinion.shutdown failed"
        )
        assert await gru.runtime_state_snapshot() == healthy_runtime_state
        await assert_orchestration_running(gru, healthy_start.orchestration_id)

        healthy_stop = await gru.stop_orchestration(healthy_start.orchestration_id)
        assert healthy_stop.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_stop_unsubscribe_failure_discards_runtime_state_after_subscription_is_removed(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(HEALTHY_PIPELINE, HEALTHY_MINION)
        assert result.success
        assert result.orchestration_id is not None
        pipeline = gru._pipelines[HEALTHY_PIPELINE]
        original_unsubscribe = pipeline._mn_unsubscribe

        async def failing_unsubscribe(detached_minion: Minion[Any, Any]) -> None:
            await original_unsubscribe(detached_minion)
            raise RuntimeError("unsubscribe boom")

        monkeypatch.setattr(pipeline, "_mn_unsubscribe", failing_unsubscribe)
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "unsubscribe boom"
        assert not await pipeline._mn_has_subscribers()
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_failed_start_discards_pipeline_resources_when_pipeline_stop_fails(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.with_fixed_resource"

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        async def failing_stop_pipeline_if_unused(_pipeline_id: str) -> None:
            raise RuntimeError("pipeline stop boom")

        monkeypatch.setattr(
            gru,
            "_stop_pipeline_if_unused",
            failing_stop_pipeline_if_unused,
        )

        failed_start = await gru.start_orchestration(
            pipeline_ref,
            "tests.assets.crash.minions.counter.boom_startup",
        )

        assert not failed_start.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_pipeline_resource_binding_failure_restores_previous_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.events.counter import CounterEvent
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as UnresourcedCounterMinion,
    )
    from tests.assets.resources.fixed.default import AssetResource as FixedResource
    from tests.assets.resources.simple.default import AssetResource as SimpleResource
    from tests.assets.support.pipeline_subscriber_ready_fixed_events import (
        SubscriberReadyFixedEventsPipeline,
    )

    class PipelineWithFailingResourceSetattr(
        SubscriberReadyFixedEventsPipeline[CounterEvent]
    ):
        fixed_resource: FixedResource
        rejected_resource: SimpleResource

        def __setattr__(self, name: str, value: object) -> None:
            if name == "rejected_resource":
                raise RuntimeError("pipeline resource injection boom")
            super().__setattr__(name, value)

        async def produce_event(self) -> CounterEvent:
            return CounterEvent(seq=await self.fixed_resource.get_value())

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        runtime_state_before = await gru.runtime_state_snapshot()

        failed_start = await gru.start_orchestration(
            PipelineWithFailingResourceSetattr,
            UnresourcedCounterMinion,
        )

        assert not failed_start.success
        assert failed_start.reason == "pipeline resource injection boom"
        assert await gru.runtime_state_snapshot() == runtime_state_before


@pytest.mark.asyncio
async def test_cancelled_start_rolls_back_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.with_fixed_resource"

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        original_acquire = gru._acquire_pipeline_resources

        async def cancel_after_acquiring_pipeline_resources(pipeline: Any) -> None:
            await original_acquire(pipeline)
            raise asyncio.CancelledError

        monkeypatch.setattr(
            gru,
            "_acquire_pipeline_resources",
            cancel_after_acquiring_pipeline_resources,
        )

        with pytest.raises(asyncio.CancelledError):
            await gru.start_orchestration(pipeline_ref, HEALTHY_MINION)

        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_cancelled_stop_finalizes_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(HEALTHY_PIPELINE, HEALTHY_MINION)
        assert result.success
        assert result.orchestration_id is not None

        async def cancelled_minion_stop(_minion: Minion[Any, Any]) -> None:
            raise asyncio.CancelledError

        monkeypatch.setattr(gru, "_stop_minion", cancelled_minion_stop)

        with pytest.raises(asyncio.CancelledError):
            await gru.stop_orchestration(result.orchestration_id)

        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_resource_startup_failure_discards_runtime_state_when_cleanup_and_logging_fail(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        original_log = gru._logger._mn_log

        async def failing_cleanup_log(level: int, msg: str, **kwargs: object) -> None:
            if msg == "Resource cleanup could not stop resources":
                raise RuntimeError("cleanup log boom")
            await original_log(level, msg, **kwargs)

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        monkeypatch.setattr(gru._logger, "_mn_log", failing_cleanup_log)

        result = await gru.start_orchestration(
            HEALTHY_PIPELINE,
            "tests.assets.crash.minions.counter.with_boom_startup_resource",
        )

        assert not result.success
        assert result.reason == (
            "tests.assets.crash.resources.boom_startup."
            "AssetResource.startup failed"
        )
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_stop_resource_cleanup_failure_discards_runtime_state_when_no_shared_owners_remain(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            HEALTHY_PIPELINE,
            "tests.assets.crash.minions.counter.with_boom_shutdown_resource",
        )
        assert result.success

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "resource cleanup boom"
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_stop_resource_cleanup_failure_preserves_shared_runtime_state_for_live_owner(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        first = await gru.start_orchestration(
            "tests.assets.pipelines.emit_one.counter.default",
            "tests.assets.minions.two_steps.counter.with_fixed_resource",
        )
        second = await gru.start_orchestration(
            "tests.assets.pipelines.emit_one.counter.default",
            "tests.assets.minions.two_steps.counter.with_fixed_resource_b",
        )
        assert first.success
        assert second.success
        assert (
            await gru.runtime_state_snapshot()
        ).resource_refcount(FIXED_RESOURCE_ID) == 2

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        stop = await gru.stop_orchestration(first.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "resource cleanup boom"
        snapshot = await gru.runtime_state_snapshot()
        assert FIXED_RESOURCE_ID in snapshot.resources
        assert snapshot.resource_refcount(FIXED_RESOURCE_ID) == 1
        assert second.orchestration_id is not None
        await assert_orchestration_running(gru, second.orchestration_id)
        await assert_runtime_component_counts_exact(gru, minions=1)


@pytest.mark.asyncio
async def test_stop_pipeline_resource_cleanup_failure_discards_runtime_state_when_no_owners_remain(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            "tests.assets.pipelines.emit_one.simple.with_simple_resource",
            "tests.assets.minions.two_steps.simple.with_simple_resource",
        )
        assert result.success

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "resource cleanup boom"
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_subscribe_failure_preserves_shared_runtime_state_for_live_owner(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        first = await gru.start_orchestration(
            "tests.assets.pipelines.emit_one.counter.default",
            "tests.assets.minions.two_steps.counter.with_fixed_resource",
        )
        assert first.success
        snapshot = await gru.runtime_state_snapshot()
        assert FIXED_RESOURCE_ID in snapshot.resources
        assert snapshot.resource_refcount(FIXED_RESOURCE_ID) == 1

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        pipeline = next(iter(gru._pipelines.values()))

        async def failing_subscribe(_minion: Minion[Any, Any]) -> None:
            raise RuntimeError("subscribe boom")

        monkeypatch.setattr(pipeline, "_mn_subscribe", failing_subscribe)
        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        second = await gru.start_orchestration(
            "tests.assets.pipelines.emit_one.counter.default",
            "tests.assets.minions.two_steps.counter.with_fixed_resource_b",
        )

        assert not second.success
        assert second.reason == "subscribe boom"
        snapshot = await gru.runtime_state_snapshot()
        assert FIXED_RESOURCE_ID in snapshot.resources
        assert snapshot.resource_refcount(FIXED_RESOURCE_ID) == 1
        assert first.orchestration_id is not None
        await assert_orchestration_running(gru, first.orchestration_id)
        await assert_runtime_component_counts_exact(gru, minions=1)


@pytest.mark.asyncio
async def test_forced_resource_discard_releases_dependency_refcounts(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.resources.fixed.default import AssetResource as FixedResource
    from tests.assets.resources.fixed.default_b import AssetResource as FixedResourceB

    async with managed_gru_context(logger=logger, metrics=metrics, state_store=state_store) as gru:
        parent_id = gru._get_resource_identity(FixedResourceB)
        dep_id = gru._get_resource_identity(FixedResource)
        await gru._ensure_resource_tree_started(FixedResource)
        await gru._ensure_resource_tree_started(FixedResourceB)

        gru._resource_dependencies[parent_id].add(dep_id)
        gru._resource_dependents[dep_id].add(parent_id)
        gru._resource_reference_counts[parent_id] = 0
        gru._resource_reference_counts[dep_id] = 1

        await gru._discard_resource_runtime_state(parent_id)

        snapshot = await gru.runtime_state_snapshot()
        assert parent_id not in snapshot.resources
        assert dep_id not in snapshot.resources
        assert snapshot.resource_refcount(parent_id) == 0
        assert snapshot.resource_refcount(dep_id) == 0
