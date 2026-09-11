import asyncio
import contextlib
from collections import Counter, defaultdict
from collections.abc import Callable
from typing import Any

import pytest

from minions import Minion, Pipeline, Resource, minion_step
from minions._internal._domain.gru import Gru
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_orchestration_running,
    assert_pipeline_resource_dependency_singletons,
    assert_pipeline_singleton,
    assert_running_minions,
    assert_runtime_component_counts_exact,
    assert_runtime_empty,
    assert_runtime_resource_maps_consistent,
    wait_for_orchestration_workflows_idle,
)
from tests.support.race_window import GatedAsyncCallable, GatedLock


@pytest.mark.asyncio
async def test_initial_minion_receives_immediately_available_first_pipeline_event(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    first_event_produced = False
    first_event_handled = asyncio.Event()
    handled: list[int] = []

    class ImmediatePipeline(Pipeline[CounterEvent]):
        async def produce_event(self) -> CounterEvent:
            nonlocal first_event_produced
            if first_event_produced:
                await asyncio.Event().wait()
            first_event_produced = True
            return CounterEvent(seq=0)

    class RecordingMinion(Minion[CounterEvent, CounterContext]):
        @minion_step
        async def record_event(self) -> None:
            handled.append(self.event.seq)
            first_event_handled.set()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(ImmediatePipeline, RecordingMinion)
        assert result.success

        await asyncio.wait_for(first_event_handled.wait(), timeout=1.0)

        assert handled == [0]


@pytest.mark.asyncio
async def test_gru_does_not_resume_same_workflow_id_twice_during_startup(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.race_cases.duplicate_workflow_resume import (
        AssetMinion as DuplicateWorkflowResumeCounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.default import (
        AssetPipeline as CounterPipeline,
    )

    DuplicateWorkflowResumeCounterMinion.reset_gates()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            minion=DuplicateWorkflowResumeCounterMinion,
            pipeline=CounterPipeline,
        )
        assert result.success
        assert DuplicateWorkflowResumeCounterMinion._gate("_startup_entered").is_set()

        def _get_step_1_workflow_ids() -> list[str]:
            return [
                log.kwargs["workflow_id"]
                for log in logger.logs
                if log.msg == "Workflow Step started"
                and log.kwargs.get("minion_id") == (
                    gru._get_minion_identity(DuplicateWorkflowResumeCounterMinion)
                )
                and log.kwargs.get("step_name") == "step_1"
            ]

        try:
            await asyncio.wait_for(
                DuplicateWorkflowResumeCounterMinion._gate("_step_1_started").wait(),
                timeout=1.0,
            )

            step_1_workflow_ids = _get_step_1_workflow_ids()
            assert step_1_workflow_ids, (
                "Expected startup-resume-race-minion to start step_1."
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
            DuplicateWorkflowResumeCounterMinion._gate("_allow_step_1_finish").set()


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_starts_for_same_orchestration(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._orchestration_locks = defaultdict(GatedLock)
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipeline),
            minion_id=gru._get_minion_identity(SimpleMinion),
            minion_config_id="",
        )
        orchestration_lock = gru._orchestration_locks[expected_orchestration_id]
        assert isinstance(orchestration_lock, GatedLock)

        task1 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipeline,
                minion=SimpleMinion,
            )
        )
        task2 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipeline,
                minion=SimpleMinion,
            )
        )

        await orchestration_lock.wait_until_held()
        await asyncio.sleep(0)
        assert orchestration_lock.enter_count == 1
        assert not task2.done()

        orchestration_lock.allow_progress()
        result1, result2 = await asyncio.gather(task1, task2)

        successes = [result for result in (result1, result2) if result.success]
        failures = [result for result in (result1, result2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Orchestration already running - start request was rejected."
        await assert_runtime_component_counts_exact(gru, minions=1)


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_stops_for_same_orchestration(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    SimplePipeline.enable_spy()
    SimplePipeline.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        start_result = await gru.start_orchestration(
            pipeline=SimplePipeline,
            minion=SimpleMinion,
        )
        assert start_result.success
        assert start_result.orchestration_id is not None
        await SimplePipeline.wait_for_calls(expected={"produce_event": 1})
        await wait_for_orchestration_workflows_idle(gru, start_result.orchestration_id)

        gru._orchestration_locks = defaultdict(GatedLock)
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipeline),
            minion_id=gru._get_minion_identity(SimpleMinion),
            minion_config_id="",
        )
        orchestration_lock = gru._orchestration_locks[expected_orchestration_id]
        assert isinstance(orchestration_lock, GatedLock)

        stop_task_1 = asyncio.create_task(
            gru.stop_orchestration(start_result.orchestration_id)
        )
        stop_task_2 = asyncio.create_task(
            gru.stop_orchestration(start_result.orchestration_id)
        )

        await orchestration_lock.wait_until_held()
        await asyncio.sleep(0)
        assert orchestration_lock.enter_count == 1
        assert not stop_task_2.done()

        orchestration_lock.allow_progress()
        stop_result_1, stop_result_2 = await asyncio.gather(stop_task_1, stop_task_2)

        successes = [result for result in (stop_result_1, stop_result_2) if result.success]
        failures = [result for result in (stop_result_1, stop_result_2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Orchestration is no longer running."
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_start_and_stop_for_same_orchestration(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    SimplePipeline.enable_spy()
    SimplePipeline.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        # Start once normally so the race only covers the stop/start window.
        start_result = await gru.start_orchestration(
            pipeline=SimplePipeline,
            minion=SimpleMinion,
        )
        assert start_result.success
        assert start_result.orchestration_id is not None
        await SimplePipeline.wait_for_calls(expected={"produce_event": 1})
        await wait_for_orchestration_workflows_idle(gru, start_result.orchestration_id)
        gru._orchestration_locks = defaultdict(GatedLock)
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipeline),
            minion_id=gru._get_minion_identity(SimpleMinion),
            minion_config_id="",
        )
        orchestration_lock = gru._orchestration_locks[expected_orchestration_id]
        assert isinstance(orchestration_lock, GatedLock)

        stop_task = asyncio.create_task(gru.stop_orchestration(start_result.orchestration_id))
        await orchestration_lock.wait_until_held()

        start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipeline,
                minion=SimpleMinion,
            )
        )

        await asyncio.sleep(0)
        assert orchestration_lock.enter_count == 1
        assert not start_task.done()

        orchestration_lock.allow_progress()
        stop_result, restart_result = await asyncio.gather(stop_task, start_task)

        assert stop_result.success
        assert restart_result.success
        await assert_runtime_component_counts_exact(gru, minions=1)


@pytest.mark.asyncio
async def test_gru_stop_waits_for_in_progress_start_of_same_orchestration(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipeline),
            minion_id=gru._get_minion_identity(SimpleMinion),
            minion_config_id="",
        )
        gated_start_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_minion", gated_start_minion)

        start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipeline,
                minion=SimpleMinion,
            )
        )
        await gated_start_minion.wait_until_called()

        stop_task = asyncio.create_task(gru.stop_orchestration(orchestration_id))
        await asyncio.sleep(0)
        stop_completed_while_start_in_progress = stop_task.done()

        gated_start_minion.allow_return()
        start_result, stop_result = await asyncio.gather(start_task, stop_task)

        assert not stop_completed_while_start_in_progress
        assert start_result.success
        assert stop_result.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_allows_concurrent_starts_for_different_orchestrations(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )
    from tests.assets.pipelines.emit_one.simple.default_b import (
        AssetPipeline as SimplePipelineB,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._orchestration_locks = defaultdict(GatedLock)
        basic_minion_id = gru._get_minion_identity(SimpleMinion)
        orchestration_id_1 = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipeline),
            minion_id=basic_minion_id,
            minion_config_id="",
        )
        orchestration_id_2 = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipelineB),
            minion_id=basic_minion_id,
            minion_config_id="",
        )
        orchestration_lock_1 = gru._orchestration_locks[orchestration_id_1]
        orchestration_lock_2 = gru._orchestration_locks[orchestration_id_2]
        assert isinstance(orchestration_lock_1, GatedLock)
        assert isinstance(orchestration_lock_2, GatedLock)

        task1 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipeline,
                minion=SimpleMinion,
            )
        )
        task2 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipelineB,
                minion=SimpleMinion,
            )
        )

        await asyncio.gather(
            orchestration_lock_1.wait_until_held(),
            orchestration_lock_2.wait_until_held(),
        )
        assert orchestration_lock_1.enter_count == 1
        assert orchestration_lock_2.enter_count == 1

        orchestration_lock_1.allow_progress()
        orchestration_lock_2.allow_progress()
        result1, result2 = await asyncio.gather(task1, task2)

        assert result1.success
        assert result2.success
        await assert_runtime_component_counts_exact(gru, minions=2)


@pytest.mark.asyncio
async def test_gru_starts_shared_resourced_pipeline_once_for_concurrent_orchestration_starts(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as CounterMinion,
    )
    from tests.assets.minions.two_steps.counter.with_file_config import (
        AssetMinion as ConfiguredCounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.with_fixed_resource import (
        AssetPipeline as FixedResourceCounterPipeline,
    )
    from tests.assets.resources.fixed.default import AssetResource as FixedResource

    FixedResourceCounterPipeline.enable_spy()
    FixedResourceCounterPipeline.reset_spy()
    FixedResource.enable_spy()
    FixedResource.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._pipeline_locks = defaultdict(GatedLock)
        pipeline_lock = gru._pipeline_locks[FixedResourceCounterPipeline.__module__]
        assert isinstance(pipeline_lock, GatedLock)

        task1 = asyncio.create_task(
            gru.start_orchestration(
                minion=CounterMinion.__module__,
                pipeline=FixedResourceCounterPipeline.__module__,
            )
        )
        task2 = asyncio.create_task(
            gru.start_orchestration(
                minion=ConfiguredCounterMinion.__module__,
                minion_config_path="tests/assets/config/minions/b.toml",
                pipeline=FixedResourceCounterPipeline.__module__,
            )
        )

        await pipeline_lock.wait_until_held()
        await asyncio.sleep(0)
        assert pipeline_lock.enter_count == 1
        assert not task2.done()

        pipeline_lock.allow_progress()
        result1, result2 = await asyncio.gather(task1, task2)

        assert result1.success
        assert result2.success
        await FixedResourceCounterPipeline.wait_for_calls(
            expected={"produce_event": 1}
        )
        assert FixedResourceCounterPipeline.get_call_counts()["_mn_startup"] == 1
        assert FixedResource.get_call_counts()["_mn_startup"] == 1
        assert sum(log.msg == "Pipeline starting" for log in logger.logs) == 1
        assert sum(log.msg == "Pipeline started" for log in logger.logs) == 1

        canonical_pipeline = gru._pipelines[FixedResourceCounterPipeline.__module__]
        assert result1.orchestration_id is not None
        assert result2.orchestration_id is not None
        assert gru._orchestrations[result1.orchestration_id].pipeline is canonical_pipeline
        assert gru._orchestrations[result2.orchestration_id].pipeline is canonical_pipeline

        resource_id = gru._get_resource_identity(FixedResource)
        await assert_runtime_component_counts_exact(
            gru,
            minions=2,
            pipelines=1,
            resources=1,
        )
        await assert_runtime_resource_maps_consistent(gru)
        snapshot = await gru.runtime_state_snapshot()
        assert snapshot.resources_for_pipeline(FixedResourceCounterPipeline.__module__) == {
            resource_id
        }
        assert snapshot.resource_refcount(resource_id) == 1

        await asyncio.gather(
            wait_for_orchestration_workflows_idle(gru, result1.orchestration_id),
            wait_for_orchestration_workflows_idle(gru, result2.orchestration_id),
        )

        stop1 = await gru.stop_orchestration(result1.orchestration_id)
        stop2 = await gru.stop_orchestration(result2.orchestration_id)

        assert stop1.success
        assert stop2.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_runtime_state_uses_singletons_for_shared_pipeline_and_resources(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.counter.with_file_config import (
        AssetMinion as ConfiguredCounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.with_resource_depending_on_fixed import (
        AssetPipeline as CounterPipelineWithResourceDependency,
    )
    from tests.assets.resources.fixed.default import AssetResource as FixedResource
    from tests.assets.resources.with_dependencies.depends_on_fixed import (
        AssetResource as ResourceDependingOnFixed,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        fixed_resource_id = gru._get_resource_identity(FixedResource)
        depends_on_fixed_resource_id = gru._get_resource_identity(ResourceDependingOnFixed)

        first_start = await gru.start_orchestration(
            pipeline=CounterPipelineWithResourceDependency.__module__,
            minion=ConfiguredCounterMinion.__module__,
            minion_config_path="tests/assets/config/minions/a.toml",
        )
        second_start = await gru.start_orchestration(
            pipeline=CounterPipelineWithResourceDependency.__module__,
            minion=ConfiguredCounterMinion.__module__,
            minion_config_path="tests/assets/config/minions/b.toml",
        )
        assert first_start.success
        assert second_start.success
        assert first_start.orchestration_id is not None
        assert second_start.orchestration_id is not None
        assert first_start.orchestration_id != second_start.orchestration_id

        snapshot = await gru.runtime_state_snapshot()
        first_minion_instance_id = snapshot.minion_instance_for_orchestration(
            first_start.orchestration_id
        )
        second_minion_instance_id = snapshot.minion_instance_for_orchestration(
            second_start.orchestration_id
        )
        assert first_minion_instance_id is not None
        assert second_minion_instance_id is not None
        assert first_minion_instance_id != second_minion_instance_id
        running_orchestrations = {
            first_start.orchestration_id: first_minion_instance_id,
            second_start.orchestration_id: second_minion_instance_id,
        }

        await assert_running_minions(
            gru,
            orchestration_to_minion_instance=running_orchestrations,
        )
        await assert_pipeline_singleton(
            gru,
            pipeline_id=CounterPipelineWithResourceDependency.__module__,
            orchestration_ids=set(running_orchestrations),
        )
        await assert_pipeline_resource_dependency_singletons(
            gru,
            pipeline_id=CounterPipelineWithResourceDependency.__module__,
            owner_resource_id=depends_on_fixed_resource_id,
            dependency_resource_id=fixed_resource_id,
            owner_refcount=1,
            dependency_refcount=1,
        )

        await wait_for_orchestration_workflows_idle(gru, first_start.orchestration_id)

        first_stop = await gru.stop_orchestration(first_start.orchestration_id)

        assert first_stop.success
        await assert_running_minions(
            gru,
            orchestration_to_minion_instance={
                second_start.orchestration_id: second_minion_instance_id
            },
        )
        await assert_pipeline_singleton(
            gru,
            pipeline_id=CounterPipelineWithResourceDependency.__module__,
            orchestration_ids={second_start.orchestration_id},
        )
        await assert_pipeline_resource_dependency_singletons(
            gru,
            pipeline_id=CounterPipelineWithResourceDependency.__module__,
            owner_resource_id=depends_on_fixed_resource_id,
            dependency_resource_id=fixed_resource_id,
            owner_refcount=1,
            dependency_refcount=1,
        )

        await wait_for_orchestration_workflows_idle(gru, second_start.orchestration_id)
        second_stop = await gru.stop_orchestration(second_start.orchestration_id)

        assert second_stop.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_concurrent_start_does_not_attach_during_last_subscriber_pipeline_shutdown(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as CounterMinion,
    )
    from tests.assets.minions.two_steps.counter.with_file_config import (
        AssetMinion as ConfiguredCounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.default import (
        AssetPipeline as CounterPipeline,
    )

    CounterPipeline.enable_spy()
    CounterPipeline.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        first = await gru.start_orchestration(
            CounterPipeline.__module__,
            CounterMinion.__module__,
        )
        assert first.success
        assert first.orchestration_id is not None
        await CounterPipeline.wait_for_calls(expected={"produce_event": 1})

        pipeline_lock = GatedLock()
        gru._pipeline_locks[CounterPipeline.__module__] = pipeline_lock
        await wait_for_orchestration_workflows_idle(gru, first.orchestration_id)
        stop_task = asyncio.create_task(gru.stop_orchestration(first.orchestration_id))
        await pipeline_lock.wait_until_held()

        start_task = asyncio.create_task(
            gru.start_orchestration(
                CounterPipeline.__module__,
                ConfiguredCounterMinion.__module__,
                minion_config_path="tests/assets/config/minions/b.toml",
            )
        )
        await asyncio.sleep(0)
        assert not start_task.done()

        pipeline_lock.allow_progress()
        stop_result, start_result = await asyncio.gather(stop_task, start_task)

        assert stop_result.success
        assert start_result.success
        assert start_result.orchestration_id is not None
        snapshot = await gru.runtime_state_snapshot()
        assert snapshot.orchestrations == {start_result.orchestration_id}
        assert snapshot.pipelines == {CounterPipeline.__module__}
        assert (
            snapshot.pipeline_for_orchestration(start_result.orchestration_id)
            == CounterPipeline.__module__
        )

        await wait_for_orchestration_workflows_idle(gru, start_result.orchestration_id)
        final_stop = await gru.stop_orchestration(start_result.orchestration_id)
        assert final_stop.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_failed_start_preserves_existing_gru_runtime_state(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        baseline = await gru.start_orchestration(
            "tests.assets.pipelines.emit_one.counter.default",
            "tests.assets.minions.one_step.counter.default",
        )
        assert baseline.success
        assert baseline.orchestration_id is not None
        expected_runtime_state = await gru.runtime_state_snapshot()

        failing_starts = (
            (
                "tests.assets.pipelines.emit_one.record.default",
                "tests.assets.minions.two_steps.counter.default",
            ),
            (
                "tests.assets.pipelines.emit_one.counter.default",
                "tests.assets.crash.minions.counter.boom_startup",
            ),
            (
                "tests.assets.crash.pipelines.counter.boom_startup",
                "tests.assets.minions.one_step.counter.default",
            ),
            (
                "tests.assets.pipelines.emit_one.counter.default",
                "tests.assets.crash.minions.counter.with_boom_startup_resource",
            ),
        )

        for pipeline_ref, minion_ref in failing_starts:
            failed = await gru.start_orchestration(pipeline_ref, minion_ref)
            assert not failed.success
            assert await gru.runtime_state_snapshot() == expected_runtime_state

        await wait_for_orchestration_workflows_idle(gru, baseline.orchestration_id)

        stopped = await gru.stop_orchestration(baseline.orchestration_id)
        assert stopped.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_failed_start_does_not_clean_up_resource_created_by_concurrent_start(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
):
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as CounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.with_fixed_resource import (
        AssetPipeline as FixedResourceCounterPipeline,
    )

    FixedResourceCounterPipeline.enable_spy()
    FixedResourceCounterPipeline.reset_spy()
    failing_minion = "tests.assets.crash.minions.counter.boom_startup"
    failing_start_reached_minion = asyncio.Event()
    allow_failing_minion_start = asyncio.Event()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        original_start_minion = gru._start_minion

        async def coordinate_start(minion: Minion[Any, Any]) -> None:
            if minion._mn_minion_module_path == failing_minion:
                failing_start_reached_minion.set()
                await allow_failing_minion_start.wait()
            await original_start_minion(minion)

        monkeypatch.setattr(gru, "_start_minion", coordinate_start)

        failed_start_task = asyncio.create_task(
            gru.start_orchestration(
                "tests.assets.pipelines.emit_one.counter.default",
                failing_minion,
            )
        )
        await asyncio.wait_for(failing_start_reached_minion.wait(), timeout=1.0)

        healthy_start = await gru.start_orchestration(
            FixedResourceCounterPipeline,
            CounterMinion,
        )
        assert healthy_start.success
        assert healthy_start.orchestration_id is not None
        await FixedResourceCounterPipeline.wait_for_calls(
            expected={"produce_event": 1}
        )

        allow_failing_minion_start.set()
        failed_start = await failed_start_task

        assert not failed_start.success
        await assert_orchestration_running(gru, healthy_start.orchestration_id)
        await assert_runtime_component_counts_exact(gru, minions=1, pipelines=1, resources=1)
        await assert_runtime_resource_maps_consistent(gru)

        await wait_for_orchestration_workflows_idle(gru, healthy_start.orchestration_id)
        healthy_stop = await gru.stop_orchestration(healthy_start.orchestration_id)
        assert healthy_stop.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_injects_resource_dependencies_before_resource_startup(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as TwoStepCounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.with_resource_depending_on_fixed import (
        AssetPipeline as CounterPipelineWithResourceDependency,
    )
    from tests.assets.resources.fixed.default import AssetResource as FixedResource
    from tests.assets.resources.with_dependencies.depends_on_fixed import (
        AssetResource as ResourceDependingOnFixed,
    )

    TwoStepCounterMinion.enable_spy()
    TwoStepCounterMinion.reset_spy()
    CounterPipelineWithResourceDependency.enable_spy()
    CounterPipelineWithResourceDependency.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            minion=TwoStepCounterMinion,
            pipeline=CounterPipelineWithResourceDependency,
        )

        assert result.success
        assert result.orchestration_id is not None
        await CounterPipelineWithResourceDependency.wait_for_calls(
            expected={"produce_event": 1}
        )
        await TwoStepCounterMinion.wait_for_calls(
            expected={"step_1": 1, "step_2": 1},
            timeout=5.0,
        )

        fixed_resource_id = gru._get_resource_identity(FixedResource)
        depends_on_fixed_resource_id = gru._get_resource_identity(ResourceDependingOnFixed)
        pipeline_id = gru._get_pipeline_identity(CounterPipelineWithResourceDependency)
        depends_on_fixed_resource_inst = gru._resources[depends_on_fixed_resource_id]
        await assert_pipeline_resource_dependency_singletons(
            gru,
            pipeline_id=pipeline_id,
            owner_resource_id=depends_on_fixed_resource_id,
            dependency_resource_id=fixed_resource_id,
            owner_refcount=1,
            dependency_refcount=1,
        )
        assert getattr(depends_on_fixed_resource_inst, "startup_value") == 123

        await wait_for_orchestration_workflows_idle(gru, result.orchestration_id)
        stop = await gru.stop_orchestration(result.orchestration_id)

        assert stop.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_starts_resource_with_multiple_resource_dependencies(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class FirstLeafResource(Resource):
        async def value(self) -> int:
            return 10

    class SecondLeafResource(Resource):
        async def value(self) -> int:
            return 20

    class MultiDependencyResource(Resource):
        first: FirstLeafResource
        second: SecondLeafResource
        startup_value: int | None = None

        async def startup(self) -> None:
            self.startup_value = await self.total()

        async def total(self) -> int:
            return await self.first.value() + await self.second.value()

    class CompoundDependencyPipeline(Pipeline[CounterEvent]):
        compound: MultiDependencyResource
        _emitted = False

        async def produce_event(self) -> CounterEvent:
            if type(self)._emitted:
                await asyncio.sleep(3600)
            type(self)._emitted = True
            return CounterEvent(seq=await self.compound.total())

    class MyMinion(Minion[CounterEvent, CounterContext]):
        @minion_step
        async def record(self) -> None:
            self.context.seq = self.event.seq

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            pipeline=CompoundDependencyPipeline,
            minion=MyMinion,
        )

        assert result.success
        assert result.orchestration_id is not None

        first_id = gru._get_resource_identity(FirstLeafResource)
        second_id = gru._get_resource_identity(SecondLeafResource)
        compound_id = gru._get_resource_identity(MultiDependencyResource)

        compound = gru._resources[compound_id]
        assert isinstance(compound, MultiDependencyResource)
        assert compound.startup_value == 30
        assert compound.first is gru._resources[first_id]
        assert compound.second is gru._resources[second_id]
        snapshot = await gru.runtime_state_snapshot()
        pipeline_id = gru._get_pipeline_identity(CompoundDependencyPipeline)
        assert snapshot.resources_for_pipeline(pipeline_id) == {compound_id}
        assert snapshot.dependencies_for_resource(compound_id) == {first_id, second_id}
        assert snapshot.resource_refcount(compound_id) == 1
        assert snapshot.resource_refcount(first_id) == 1
        assert snapshot.resource_refcount(second_id) == 1

        await wait_for_orchestration_workflows_idle(gru, result.orchestration_id)
        stop = await gru.stop_orchestration(result.orchestration_id)

        assert stop.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_start_fails_clearly_for_circular_resource_dependencies(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    class CycleAResource(Resource):
        pass

    class CycleBResource(Resource):
        a: CycleAResource

    # Keep the cycle fixture local while mimicking a resolved postponed annotation.
    CycleAResource.__annotations__ = {"b": CycleBResource}

    class CircularResourcePipeline(Pipeline[CounterEvent]):
        a: CycleAResource

        async def produce_event(self) -> CounterEvent:
            return CounterEvent(seq=1)

    class MyMinion(Minion[CounterEvent, CounterContext]):
        @minion_step
        async def record(self) -> None:
            self.context.seq = self.event.seq

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            pipeline=CircularResourcePipeline,
            minion=MyMinion,
        )

        assert not result.success
        assert result.reason == "Cycle detected in Resource dependencies"
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_allows_concurrent_starts_for_different_orchestrations_while_stop_is_in_flight(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )
    from tests.assets.pipelines.emit_one.simple.default_b import (
        AssetPipeline as SimplePipelineB,
    )

    SimplePipeline.enable_spy()
    SimplePipeline.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        orchestration1_start_result = await gru.start_orchestration(
            pipeline=SimplePipeline,
            minion=SimpleMinion,
        )
        assert orchestration1_start_result.success
        assert orchestration1_start_result.orchestration_id is not None
        await SimplePipeline.wait_for_calls(expected={"produce_event": 1})
        await wait_for_orchestration_workflows_idle(
            gru, orchestration1_start_result.orchestration_id
        )

        gated_orchestration1_stop = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_stop_minion", gated_orchestration1_stop)

        orchestration1_stop_task = asyncio.create_task(
            gru.stop_orchestration(orchestration1_start_result.orchestration_id)
        )
        await gated_orchestration1_stop.wait_until_called()

        orchestration2_start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipelineB,
                minion=SimpleMinion,
            )
        )

        orchestration2_start_result = await asyncio.wait_for(
            orchestration2_start_task, timeout=10.0
        )
        assert orchestration2_start_result.success
        assert orchestration2_start_result.orchestration_id is not None
        await assert_orchestration_running(gru, orchestration2_start_result.orchestration_id)
        orchestration2_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity(SimplePipelineB),
            minion_id=gru._get_minion_identity(SimpleMinion),
            minion_config_id="",
        )
        await assert_orchestration_running(gru, orchestration2_id)
        assert orchestration1_start_result.orchestration_id is not None
        await assert_orchestration_running(gru, orchestration1_start_result.orchestration_id)
        await assert_runtime_component_counts_exact(gru, minions=2)

        gated_orchestration1_stop.allow_return()
        orchestration1_stop_result = await orchestration1_stop_task

        assert orchestration1_stop_result.success


@pytest.mark.asyncio
async def test_gru_shutdown_advertises_shutdown_before_waiting(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):

    async with managed_gru_context(
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
async def test_cancelled_shutdown_request_still_completes_terminal_shutdown(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        # Keep shutdown pending until its caller is cancelled.
        async with gru._lifecycle_ops_state_lock:
            shutdown_task = asyncio.create_task(gru.shutdown())
            await asyncio.sleep(0)
            assert gru._is_shutting_down
            assert not shutdown_task.done()
            terminal_shutdown_task = gru._shutdown_task
            assert terminal_shutdown_task is not None

            shutdown_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await shutdown_task

        shutdown_result = await terminal_shutdown_task
        assert shutdown_result.success
        assert gru._is_shutdown


@pytest.mark.asyncio
async def test_gru_shutdown_waits_for_in_flight_start_orchestration(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gated_start_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_minion", gated_start_minion)

        start_task = asyncio.create_task(
            gru.start_orchestration(
                minion=SimpleMinion,
                pipeline=SimplePipeline,
            )
        )

        await gated_start_minion.wait_until_called()

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task.done()

        gated_start_minion.allow_return()
        start_result, shutdown_result = await asyncio.gather(start_task, shutdown_task)

        assert start_result.success
        assert shutdown_result.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_shutdown_waits_for_in_flight_stop_orchestration(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    SimplePipeline.enable_spy()
    SimplePipeline.reset_spy()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        start_result = await gru.start_orchestration(
            minion=SimpleMinion,
            pipeline=SimplePipeline,
        )
        assert start_result.success
        assert start_result.orchestration_id is not None
        await SimplePipeline.wait_for_calls(expected={"produce_event": 1})
        await wait_for_orchestration_workflows_idle(gru, start_result.orchestration_id)

        gated_stop_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_stop_minion", gated_stop_minion)

        stop_task = asyncio.create_task(gru.stop_orchestration(start_result.orchestration_id))
        await gated_stop_minion.wait_until_called()

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task.done()

        gated_stop_minion.allow_return()
        stop_result, shutdown_result = await asyncio.gather(stop_task, shutdown_task)

        assert stop_result.success
        assert shutdown_result.success
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_shutdown_rejects_new_lifecycle_work_while_shutdown_is_pending(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    async with managed_gru_context(
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
                gru.start_orchestration(
                    minion=SimpleMinion,
                    pipeline=SimplePipeline,
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
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        start_result = await gru.start_orchestration(
            pipeline=SimplePipeline,
            minion=SimpleMinion,
        )
        assert start_result.success
        assert start_result.orchestration_id is not None

        gated_shutdown_async_component = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_shutdown_async_component", gated_shutdown_async_component)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await gated_shutdown_async_component.wait_until_called()

        new_start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=SimplePipeline,
                minion=SimpleMinion,
            )
        )
        new_stop_task = asyncio.create_task(
            gru.stop_orchestration(start_result.orchestration_id)
        )

        start_result2, stop_result2 = await asyncio.gather(new_start_task, new_stop_task)
        assert not start_result2.success
        assert start_result2.reason == "Gru is shutting down."
        assert not stop_result2.success
        assert stop_result2.reason == "Gru is shutting down."

        gated_shutdown_async_component.allow_return()
        shutdown_result = await shutdown_task
        assert shutdown_result.success


@pytest.mark.asyncio
async def test_pending_failure_finalization_waits_for_first_and_blocks_second_lifecycle_operation(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    first_lifecycle_reservation_acquired = asyncio.Event()
    first_lifecycle_reservation_release = asyncio.Event()

    failure_finalization_reservation_requested = asyncio.Event()
    failure_finalization_reservation_acquired = asyncio.Event()
    failure_finalization_reservation_release = asyncio.Event()

    second_lifecycle_reservation_requested = asyncio.Event()
    second_lifecycle_reservation_acquired = asyncio.Event()

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        async def hold_first_lifecycle_reservation() -> None:
            async with gru._reserve_lifecycle_op() as reserved:
                assert reserved
                first_lifecycle_reservation_acquired.set()
                await first_lifecycle_reservation_release.wait()

        async def hold_failure_finalization_reservation() -> None:
            failure_finalization_reservation_requested.set()
            async with gru._reserve_runtime_failure_finalization() as reserved:
                assert reserved
                failure_finalization_reservation_acquired.set()
                await failure_finalization_reservation_release.wait()

        async def acquire_second_lifecycle_reservation() -> None:
            second_lifecycle_reservation_requested.set()
            async with gru._reserve_lifecycle_op() as reserved:
                assert reserved
                second_lifecycle_reservation_acquired.set()

        async with asyncio.TaskGroup() as task_group:
            task_group.create_task(hold_first_lifecycle_reservation())
            await asyncio.wait_for(
                first_lifecycle_reservation_acquired.wait(),
                timeout=1.0,
            )

            task_group.create_task(hold_failure_finalization_reservation())
            await asyncio.wait_for(
                failure_finalization_reservation_requested.wait(),
                timeout=1.0,
            )
            assert gru._runtime_failure_finalization_waiter_count == 1

            task_group.create_task(acquire_second_lifecycle_reservation())
            await asyncio.wait_for(
                second_lifecycle_reservation_requested.wait(),
                timeout=1.0,
            )
            assert not second_lifecycle_reservation_acquired.is_set()

            first_lifecycle_reservation_release.set()
            await asyncio.wait_for(
                failure_finalization_reservation_acquired.wait(),
                timeout=1.0,
            )
            assert not second_lifecycle_reservation_acquired.is_set()

            failure_finalization_reservation_release.set()
            await asyncio.wait_for(
                second_lifecycle_reservation_acquired.wait(),
                timeout=1.0,
            )


@pytest.mark.asyncio
async def test_gru_shutdown_serializes_concurrent_shutdown_calls(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SimplePipeline,
    )

    async with managed_gru_context(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gated_start_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_minion", gated_start_minion)

        start_task = asyncio.create_task(
            gru.start_orchestration(
                minion=SimpleMinion,
                pipeline=SimplePipeline,
            )
        )

        await gated_start_minion.wait_until_called()

        assert not gru._is_shutting_down
        shutdown_task_1 = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert gru._is_shutting_down
        assert not shutdown_task_1.done()

        shutdown_task_2 = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task_2.done()

        gated_start_minion.allow_return()
        start_result, shutdown_result_1, shutdown_result_2 = await asyncio.gather(
            start_task,
            shutdown_task_1,
            shutdown_task_2,
        )

        assert start_result.success
        assert shutdown_result_1.success
        assert shutdown_result_2.success
        await assert_runtime_empty(gru)
