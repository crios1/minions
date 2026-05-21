import asyncio
import contextlib
import importlib
from collections import Counter, defaultdict
from collections.abc import Callable

import pytest

from minions._internal._domain.gru import Gru

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.support.race_window import GatedAsyncCallable, GatedLock


@pytest.mark.asyncio
async def test_gru_does_not_replay_same_workflow_id_during_startup(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
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


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_starts_for_same_minion(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._minion_locks = defaultdict(GatedLock)
        minion_key = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            None,
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        minion_gated_lock = gru._minion_locks[minion_key]
        assert isinstance(minion_gated_lock, GatedLock)

        task1 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )
        task2 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )

        await asyncio.wait_for(minion_gated_lock.entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert minion_gated_lock.enter_count == 1
        assert not task2.done()

        minion_gated_lock.release_gate.set()
        result1, result2 = await asyncio.gather(task1, task2)

        successes = [result for result in (result1, result2) if result.success]
        failures = [result for result in (result1, result2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Minion already running — start request was rejected."
        assert len(gru._minions_by_composite_key) == 1


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_stops_for_same_minion(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        start_result = await gru.start_minion(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        assert start_result.success

        gru._minion_locks = defaultdict(GatedLock)
        minion_key = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            None,
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        minion_gated_lock = gru._minion_locks[minion_key]
        assert isinstance(minion_gated_lock, GatedLock)

        stop_task_1 = asyncio.create_task(gru.stop_minion(start_result.instance_id or ""))
        stop_task_2 = asyncio.create_task(gru.stop_minion(start_result.instance_id or ""))

        await asyncio.wait_for(minion_gated_lock.entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert minion_gated_lock.enter_count == 1
        assert not stop_task_2.done()

        minion_gated_lock.release_gate.set()
        stop_result_1, stop_result_2 = await asyncio.gather(stop_task_1, stop_task_2)

        successes = [result for result in (stop_result_1, stop_result_2) if result.success]
        failures = [result for result in (stop_result_1, stop_result_2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Minion is no longer running."
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_start_and_stop_for_same_minion(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        # Start once normally so the race only covers the stop/start window.
        start_result = await gru.start_minion(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        assert start_result.success
        gru._minion_locks = defaultdict(GatedLock)
        minion_key = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            None,
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        minion_gated_lock = gru._minion_locks[minion_key]
        assert isinstance(minion_gated_lock, GatedLock)

        stop_task = asyncio.create_task(gru.stop_minion(start_result.instance_id or ""))
        await asyncio.wait_for(minion_gated_lock.entered.wait(), timeout=1.0)

        start_task = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )

        await asyncio.sleep(0)
        assert minion_gated_lock.enter_count == 1
        assert not start_task.done()

        minion_gated_lock.release_gate.set()
        stop_result, restart_result = await asyncio.gather(stop_task, start_task)

        assert stop_result.success
        assert restart_result.success
        assert len(gru._minions_by_composite_key) == 1


@pytest.mark.asyncio
async def test_gru_allows_concurrent_starts_for_different_minions(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._minion_locks = defaultdict(GatedLock)
        minion_key_1 = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            "tests/assets/config/minions/a.toml",
            "tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        minion_key_2 = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            "tests/assets/config/minions/b.toml",
            "tests.assets.pipelines.simple.simple_event.single_event_2",
        )
        gate1 = gru._minion_locks[minion_key_1]
        gate2 = gru._minion_locks[minion_key_2]
        assert isinstance(gate1, GatedLock)
        assert isinstance(gate2, GatedLock)

        task1 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path="tests/assets/config/minions/a.toml",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )
        task2 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path="tests/assets/config/minions/b.toml",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_2",
            )
        )

        await asyncio.wait_for(asyncio.gather(gate1.entered.wait(), gate2.entered.wait()), timeout=1.0)
        await asyncio.sleep(0)
        assert gate1.enter_count == 1
        assert gate2.enter_count == 1

        gate1.release_gate.set()
        gate2.release_gate.set()
        result1, result2 = await asyncio.gather(task1, task2)

        assert result1.success
        assert result2.success
        assert len(gru._minions_by_composite_key) == 2


@pytest.mark.asyncio
async def test_gru_starts_shared_resourced_pipeline_once_for_concurrent_minion_starts(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    resourced_pipeline = importlib.import_module(
        "tests.assets.pipelines.resourced.counter.with_fixed_resource"
    )
    resourced_pipeline.ResourcedPipeline.enable_spy()
    resourced_pipeline.ResourcedPipeline.reset_spy()

    fixed_resource = importlib.import_module("tests.assets.resources.fixed.base")
    fixed_resource.FixedResource.enable_spy()
    fixed_resource.FixedResource.reset_spy()

    pipeline_modpath = "tests.assets.pipelines.resourced.counter.with_fixed_resource"

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._pipeline_locks = defaultdict(GatedLock)
        pipeline_gate = gru._pipeline_locks[pipeline_modpath]
        assert isinstance(pipeline_gate, GatedLock)

        task1 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.counter.basic",
                minion_config_path="tests/assets/config/minions/a.toml",
                pipeline=pipeline_modpath,
            )
        )
        task2 = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.counter.basic",
                minion_config_path="tests/assets/config/minions/b.toml",
                pipeline=pipeline_modpath,
            )
        )

        await asyncio.wait_for(pipeline_gate.entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert pipeline_gate.enter_count == 1
        assert not task2.done()

        pipeline_gate.release_gate.set()
        result1, result2 = await asyncio.gather(task1, task2)

        assert result1.success
        assert result2.success
        assert len(gru._pipelines) == 1
        assert len(gru._resources) == 1
        assert resourced_pipeline.ResourcedPipeline.get_call_counts()["_mn_startup"] == 1
        assert fixed_resource.FixedResource.get_call_counts()["_mn_startup"] == 1

        resource_id = gru._make_resource_id(fixed_resource.FixedResource)
        assert gru._pipeline_resource_map[pipeline_modpath] == {resource_id}
        assert gru._resource_refcounts[resource_id] == 1

        stop1 = await gru.stop_minion(result1.instance_id or "")
        stop2 = await gru.stop_minion(result2.instance_id or "")

        assert stop1.success
        assert stop2.success
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_gru_allows_concurrent_starts_for_different_minions_while_stop_is_in_flight(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        minion1_start_result = await gru.start_minion(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        assert minion1_start_result.success

        minion1_stop_gate = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_stop_minion", minion1_stop_gate)

        minion1_stop_task = asyncio.create_task(gru.stop_minion(minion1_start_result.instance_id or ""))
        await asyncio.wait_for(minion1_stop_gate.entered.wait(), timeout=1.0)

        minion2_start_task = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_2",
            )
        )

        minion2_start_result = await asyncio.wait_for(minion2_start_task, timeout=1.0)
        assert minion2_start_result.success
        assert minion2_start_result.instance_id is not None
        assert minion2_start_result.instance_id in gru._minions_by_id
        minion2_key = gru._make_minion_composite_key(
            "tests.assets.minions.two_steps.simple.basic",
            None,
            "tests.assets.pipelines.simple.simple_event.single_event_2",
        )
        assert minion2_key in gru._minions_by_composite_key
        assert minion1_start_result.instance_id in gru._minions_by_id
        assert len(gru._minions_by_composite_key) == 2

        minion1_stop_gate.release.set()
        minion1_stop_result = await minion1_stop_task

        assert minion1_stop_result.success


@pytest.mark.asyncio
async def test_gru_shutdown_advertises_shutdown_before_waiting(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        # Hold the lifecycle lock so shutdown stays in the pending window deterministically.
        await gru._lifecycle_ops_state_lock.acquire()
        try:
            assert not gru._is_shutting_down
            shutdown_task = asyncio.create_task(gru.shutdown())
            await asyncio.sleep(0)
            assert gru._is_shutting_down
            assert not shutdown_task.done()
        finally:
            gru._lifecycle_ops_state_lock.release()

        shutdown_result = await shutdown_task
        assert shutdown_result.success


@pytest.mark.asyncio
async def test_gru_shutdown_waits_for_in_flight_start_minion(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gated__start_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_minion", gated__start_minion)

        start_task = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )

        await asyncio.wait_for(gated__start_minion.entered.wait(), timeout=1.0)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task.done()

        gated__start_minion.release.set()
        start_result, shutdown_result = await asyncio.gather(start_task, shutdown_task)

        assert start_result.success
        assert shutdown_result.success
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_gru_shutdown_waits_for_in_flight_stop_minion(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        start_result = await gru.start_minion(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        assert start_result.success

        gated__stop_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_stop_minion", gated__stop_minion)

        stop_task = asyncio.create_task(gru.stop_minion(start_result.instance_id or ""))
        await asyncio.wait_for(gated__stop_minion.entered.wait(), timeout=1.0)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task.done()

        gated__stop_minion.release.set()
        stop_result, shutdown_result = await asyncio.gather(stop_task, shutdown_task)

        assert stop_result.success
        assert shutdown_result.success
        assert gru._runtime_state_snapshot() == {}


@pytest.mark.asyncio
async def test_gru_shutdown_rejects_new_lifecycle_work_while_shutdown_is_pending(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        # Hold the lifecycle lock so shutdown stays pending and new lifecycle work
        # must contend with the shutdown-in-progress state.
        await gru._lifecycle_ops_state_lock.acquire()
        try:
            shutdown_task = asyncio.create_task(gru.shutdown())
            await asyncio.sleep(0)
            assert gru._is_shutting_down
            assert not shutdown_task.done()

            start_task = asyncio.create_task(
                gru.start_minion(
                    minion="tests.assets.minions.two_steps.simple.basic",
                    pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
                )
            )
            await asyncio.sleep(0)
            assert not start_task.done()
        finally:
            gru._lifecycle_ops_state_lock.release()

        start_result, shutdown_result = await asyncio.gather(start_task, shutdown_task)

        assert not start_result.success
        assert start_result.reason == "Gru is shutting down."
        assert shutdown_result.success


@pytest.mark.asyncio
async def test_gru_shutdown_rejects_new_lifecycle_work_during_shutdown_in_progress(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        start_result = await gru.start_minion(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
        )
        assert start_result.success

        gated__shutdown_async_component = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_shutdown_async_component", gated__shutdown_async_component)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.wait_for(gated__shutdown_async_component.entered.wait(), timeout=1.0)

        new_start_task = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )
        new_stop_task = asyncio.create_task(gru.stop_minion(start_result.instance_id or ""))

        start_result2, stop_result2 = await asyncio.gather(new_start_task, new_stop_task)
        assert not start_result2.success
        assert start_result2.reason == "Gru is shutting down."
        assert not stop_result2.success
        assert stop_result2.reason == "Gru is shutting down."

        gated__shutdown_async_component.release.set()
        shutdown_result = await shutdown_task
        assert shutdown_result.success


@pytest.mark.asyncio
async def test_gru_shutdown_serializes_concurrent_shutdown_calls(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gated__start_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_minion", gated__start_minion)

        start_task = asyncio.create_task(
            gru.start_minion(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline="tests.assets.pipelines.simple.simple_event.single_event_1",
            )
        )

        await asyncio.wait_for(gated__start_minion.entered.wait(), timeout=1.0)

        assert not gru._is_shutting_down
        shutdown_task_1 = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert gru._is_shutting_down
        assert not shutdown_task_1.done()

        shutdown_task_2 = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task_2.done()

        gated__start_minion.release.set()
        start_result, shutdown_result_1, shutdown_result_2 = await asyncio.gather(
            start_task,
            shutdown_task_1,
            shutdown_task_2,
        )

        assert start_result.success
        assert shutdown_result_1.success
        assert shutdown_result_2.success
        assert gru._runtime_state_snapshot() == {}
