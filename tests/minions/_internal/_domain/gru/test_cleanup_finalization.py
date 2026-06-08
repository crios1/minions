import contextlib
from collections.abc import Callable
from typing import Any

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

GOOD_MINION = "tests.assets.crash.minions.good"
GOOD_PIPELINE = "tests.assets.crash.pipelines.healthy_counter"
FIXED_RESOURCE_ID = "tests.assets.resources.fixed.base.FixedResource"


@pytest.mark.asyncio
async def test_stop_committed_minion_shutdown_failure_discards_runtime_state_when_cleanup_helper_fails(  # noqa: E501
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            GOOD_PIPELINE, "tests.assets.crash.minions.boom_shutdown"
        )
        assert result.success

        async def failing_stop_orchestration_best_effort(_minion: Minion[Any, Any]) -> None:
            raise RuntimeError("cleanup helper boom")

        monkeypatch.setattr(
            gru, "_stop_orchestration_best_effort", failing_stop_orchestration_best_effort
        )
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert (
            stop.reason
            == "BoomShutdownMinion.shutdown failed (tests/assets/crash/minions/boom_shutdown.py)"
        )
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_stop_unsubscribe_failure_discards_runtime_state_after_subscription_is_removed(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(GOOD_PIPELINE, GOOD_MINION)
        assert result.success
        assert result.orchestration_id is not None
        minion = gru._minions_by_orchestration_id[result.orchestration_id]
        pipeline = gru._pipelines[GOOD_PIPELINE]

        async def failing_unsubscribe(detached_minion: Minion[Any, Any]) -> None:
            async with pipeline._mn_subs_lock:
                pipeline._mn_subs.discard(detached_minion)
            raise RuntimeError("unsubscribe boom")

        monkeypatch.setattr(pipeline, "_mn_unsubscribe", failing_unsubscribe)
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "unsubscribe boom"
        assert minion not in pipeline._mn_subs
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_start_resource_startup_failure_discards_runtime_state_when_cleanup_and_logging_fail(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:

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
            GOOD_PIPELINE,
            "tests.assets.crash.minions.depends_on_boom_startup_resource",
        )

        assert not result.success
        assert (
            result.reason
            == "BoomStartupResource.startup failed (tests/assets/crash/resources/boom_startup.py)"
        )
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_stop_resource_cleanup_failure_discards_runtime_state_when_no_shared_owners_remain(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            GOOD_PIPELINE,
            "tests.assets.crash.minions.depends_on_boom_shutdown_resource",
        )
        assert result.success

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "resource cleanup boom"
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_stop_resource_cleanup_failure_preserves_shared_runtime_state_for_live_owner(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        first = await gru.start_orchestration(
            "tests.assets.pipelines.emit1.counter.emit_1",
            "tests.assets.minions.two_steps.counter.resourced",
        )
        second = await gru.start_orchestration(
            "tests.assets.pipelines.emit1.counter.emit_1",
            "tests.assets.minions.two_steps.counter.resourced_shared_b",
        )
        assert first.success
        assert second.success
        assert gru._resource_refcounts[FIXED_RESOURCE_ID] == 2

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        stop = await gru.stop_orchestration(first.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "resource cleanup boom"
        assert FIXED_RESOURCE_ID in gru._resources
        assert gru._resource_refcounts[FIXED_RESOURCE_ID] == 1
        assert len(gru._minions_by_instance_id) == 1
        assert second.orchestration_id in gru._minions_by_orchestration_id


@pytest.mark.asyncio
async def test_stop_pipeline_resource_cleanup_failure_discards_runtime_state_when_no_owners_remain(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        result = await gru.start_orchestration(
            "tests.assets.pipelines.simple.simple_event.resourced",
            "tests.assets.minions.two_steps.simple.resourced_1",
        )
        assert result.success

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert not stop.success
        assert stop.reason == "resource cleanup boom"
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_start_subscribe_failure_preserves_shared_runtime_state_for_live_owner(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        first = await gru.start_orchestration(
            "tests.assets.pipelines.emit1.counter.emit_1",
            "tests.assets.minions.two_steps.counter.resourced",
        )
        assert first.success
        assert FIXED_RESOURCE_ID in gru._resources
        assert gru._resource_refcounts[FIXED_RESOURCE_ID] == 1

        async def failing_cleanup_resources(_resource_ids: set[str]) -> None:
            raise RuntimeError("resource cleanup boom")

        pipeline = next(iter(gru._pipelines.values()))

        async def failing_subscribe(_minion: Minion[Any, Any]) -> None:
            raise RuntimeError("subscribe boom")

        monkeypatch.setattr(pipeline, "_mn_subscribe", failing_subscribe)
        monkeypatch.setattr(gru, "_cleanup_resources", failing_cleanup_resources)
        second = await gru.start_orchestration(
            "tests.assets.pipelines.emit1.counter.emit_1",
            "tests.assets.minions.two_steps.counter.resourced_shared_b",
        )

        assert not second.success
        assert second.reason == "subscribe boom"
        assert FIXED_RESOURCE_ID in gru._resources
        assert gru._resource_refcounts[FIXED_RESOURCE_ID] == 1
        assert len(gru._minions_by_instance_id) == 1
        assert first.orchestration_id in gru._minions_by_orchestration_id


@pytest.mark.asyncio
async def test_forced_resource_discard_releases_dependency_refcounts(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.resources.fixed.base import FixedResource
    from tests.assets.resources.fixed.base_b import FixedResourceB

    async with gru_factory(logger=logger, metrics=metrics, state_store=state_store) as gru:
        parent_id = gru._make_resource_id(FixedResourceB)
        dep_id = gru._make_resource_id(FixedResource)
        await gru._ensure_resource_tree_started(FixedResource)
        await gru._ensure_resource_tree_started(FixedResourceB)

        gru._resource_dependencies[parent_id].add(dep_id)
        gru._resource_dependents[dep_id].add(parent_id)
        gru._resource_refcounts[parent_id] = 0
        gru._resource_refcounts[dep_id] = 1

        await gru._discard_resource_runtime_state(parent_id)

        assert parent_id not in gru._resources
        assert dep_id not in gru._resources
        assert parent_id not in gru._resource_refcounts
        assert dep_id not in gru._resource_refcounts
