import asyncio
import contextlib
import importlib
from collections import Counter, defaultdict
from collections.abc import Callable

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
)
from tests.support.race_window import GatedAsyncCallable, GatedLock


@pytest.mark.asyncio
async def test_gru_does_not_replay_same_workflow_id_during_startup(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.minions.race_cases.duplicate_workflow_replay import (
        AssetMinion as DuplicateWorkflowReplayMinion,
    )

    DuplicateWorkflowReplayMinion.reset_gates()

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            minion="tests.assets.minions.race_cases.duplicate_workflow_replay",
            pipeline="tests.assets.pipelines.emit_one.counter.default",
        )
        assert result.success
        assert DuplicateWorkflowReplayMinion._gate("_startup_entered").is_set()

        def _get_step_1_workflow_ids() -> list[str]:
            return [
                log.kwargs["workflow_id"]
                for log in logger.logs
                if log.msg == "Workflow Step started"
                and log.kwargs.get("minion_id") == (
                    gru._get_minion_identity_from_module_path(
                        "tests.assets.minions.race_cases.duplicate_workflow_replay"
                    )
                )
                and log.kwargs.get("step_name") == "step_1"
            ]

        try:
            await asyncio.wait_for(
                DuplicateWorkflowReplayMinion._gate("_step_1_started").wait(),
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
            DuplicateWorkflowReplayMinion._gate("_allow_step_1_finish").set()


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_starts_for_same_orchestration(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        gru._orchestration_locks = defaultdict(GatedLock)
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity_from_module_path(
                pipeline_ref,
            ),
            minion_id=gru._get_minion_identity_from_module_path(
                minion_ref,
            ),
            minion_config_id="",
        )
        orchestration_gated_lock = gru._orchestration_locks[expected_orchestration_id]
        assert isinstance(orchestration_gated_lock, GatedLock)

        task1 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_ref,
                minion=minion_ref,
            )
        )
        task2 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_ref,
                minion=minion_ref,
            )
        )

        await asyncio.wait_for(orchestration_gated_lock.entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert orchestration_gated_lock.enter_count == 1
        assert not task2.done()

        orchestration_gated_lock.release_gate.set()
        result1, result2 = await asyncio.gather(task1, task2)

        successes = [result for result in (result1, result2) if result.success]
        failures = [result for result in (result1, result2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Orchestration already running - start request was rejected."
        assert_runtime_component_counts_exact(gru, minions=1)


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_stops_for_same_orchestration(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        start_result = await gru.start_orchestration(
            pipeline=pipeline_ref,
            minion=minion_ref,
        )
        assert start_result.success

        gru._orchestration_locks = defaultdict(GatedLock)
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity_from_module_path(
                pipeline_ref,
            ),
            minion_id=gru._get_minion_identity_from_module_path(
                minion_ref,
            ),
            minion_config_id="",
        )
        orchestration_gated_lock = gru._orchestration_locks[expected_orchestration_id]
        assert isinstance(orchestration_gated_lock, GatedLock)

        stop_task_1 = asyncio.create_task(
            gru.stop_orchestration(start_result.orchestration_id or "")
        )
        stop_task_2 = asyncio.create_task(
            gru.stop_orchestration(start_result.orchestration_id or "")
        )

        await asyncio.wait_for(orchestration_gated_lock.entered.wait(), timeout=1.0)
        await asyncio.sleep(0)
        assert orchestration_gated_lock.enter_count == 1
        assert not stop_task_2.done()

        orchestration_gated_lock.release_gate.set()
        stop_result_1, stop_result_2 = await asyncio.gather(stop_task_1, stop_task_2)

        successes = [result for result in (stop_result_1, stop_result_2) if result.success]
        failures = [result for result in (stop_result_1, stop_result_2) if not result.success]

        assert len(successes) == 1
        assert len(failures) == 1
        assert failures[0].reason == "Minion is no longer running."
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_serializes_concurrent_start_and_stop_for_same_orchestration(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        # Start once normally so the race only covers the stop/start window.
        start_result = await gru.start_orchestration(
            pipeline=pipeline_ref,
            minion=minion_ref,
        )
        assert start_result.success
        gru._orchestration_locks = defaultdict(GatedLock)
        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity_from_module_path(
                pipeline_ref,
            ),
            minion_id=gru._get_minion_identity_from_module_path(
                minion_ref,
            ),
            minion_config_id="",
        )
        orchestration_gated_lock = gru._orchestration_locks[expected_orchestration_id]
        assert isinstance(orchestration_gated_lock, GatedLock)

        stop_task = asyncio.create_task(gru.stop_orchestration(start_result.orchestration_id or ""))
        await asyncio.wait_for(orchestration_gated_lock.entered.wait(), timeout=1.0)

        start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_ref,
                minion=minion_ref,
            )
        )

        await asyncio.sleep(0)
        assert orchestration_gated_lock.enter_count == 1
        assert not start_task.done()

        orchestration_gated_lock.release_gate.set()
        stop_result, restart_result = await asyncio.gather(stop_task, start_task)

        assert stop_result.success
        assert restart_result.success
        assert_runtime_component_counts_exact(gru, minions=1)


@pytest.mark.asyncio
async def test_gru_allows_concurrent_starts_for_different_orchestrations(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        pipeline_b_ref = "tests.assets.pipelines.emit_one.simple.default_b"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        gru._orchestration_locks = defaultdict(GatedLock)
        basic_minion_id = gru._get_minion_identity_from_module_path(
            minion_ref,
        )
        orchestration_id_1 = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity_from_module_path(
                pipeline_ref,
            ),
            minion_id=basic_minion_id,
            minion_config_id="",
        )
        orchestration_id_2 = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity_from_module_path(
                pipeline_b_ref,
            ),
            minion_id=basic_minion_id,
            minion_config_id="",
        )
        gate1 = gru._orchestration_locks[orchestration_id_1]
        gate2 = gru._orchestration_locks[orchestration_id_2]
        assert isinstance(gate1, GatedLock)
        assert isinstance(gate2, GatedLock)

        task1 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_ref,
                minion=minion_ref,
            )
        )
        task2 = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_b_ref,
                minion=minion_ref,
            )
        )

        await asyncio.wait_for(
            asyncio.gather(gate1.entered.wait(), gate2.entered.wait()),
            timeout=1.0
        )
        await asyncio.sleep(0)
        assert gate1.enter_count == 1
        assert gate2.enter_count == 1

        gate1.release_gate.set()
        gate2.release_gate.set()
        result1, result2 = await asyncio.gather(task1, task2)

        assert result1.success
        assert result2.success
        assert_runtime_component_counts_exact(gru, minions=2)


@pytest.mark.asyncio
async def test_gru_starts_shared_resourced_pipeline_once_for_concurrent_orchestration_starts(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    fixed_resource_pipeline_module = importlib.import_module(
        "tests.assets.pipelines.emit_one.counter.with_fixed_resource"
    )
    fixed_resource_pipeline_module.AssetPipeline.enable_spy()
    fixed_resource_pipeline_module.AssetPipeline.reset_spy()

    fixed_resource_module = importlib.import_module("tests.assets.resources.fixed.default")
    fixed_resource_module.AssetResource.enable_spy()
    fixed_resource_module.AssetResource.reset_spy()

    pipeline_module_path = "tests.assets.pipelines.emit_one.counter.with_fixed_resource"

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gru._pipeline_locks = defaultdict(GatedLock)
        pipeline_gate = gru._pipeline_locks[pipeline_module_path]
        assert isinstance(pipeline_gate, GatedLock)

        task1 = asyncio.create_task(
            gru.start_orchestration(
                minion="tests.assets.minions.two_steps.counter.default",
                pipeline=pipeline_module_path,
            )
        )
        task2 = asyncio.create_task(
            gru.start_orchestration(
                minion="tests.assets.minions.two_steps.counter.with_file_config",
                minion_config_path="tests/assets/config/minions/b.toml",
                pipeline=pipeline_module_path,
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
        assert fixed_resource_pipeline_module.AssetPipeline.get_call_counts()["_mn_startup"] == 1
        assert fixed_resource_module.AssetResource.get_call_counts()["_mn_startup"] == 1

        resource_id = gru._get_resource_identity(fixed_resource_module.AssetResource)
        assert_runtime_component_counts_exact(gru, pipelines=1, resources=1)
        assert_runtime_resource_maps_consistent(gru)
        snapshot = gru.runtime_state_snapshot()
        assert snapshot.resources_for_pipeline(pipeline_module_path) == {resource_id}
        assert snapshot.resource_refcount(resource_id) == 1

        stop1 = await gru.stop_orchestration(result1.orchestration_id or "")
        stop2 = await gru.stop_orchestration(result2.orchestration_id or "")

        assert stop1.success
        assert stop2.success
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_runtime_state_uses_singletons_for_shared_pipeline_and_resources(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.resources.fixed.default import AssetResource as FixedResource
    from tests.assets.resources.with_dependencies.depends_on_fixed import (
        AssetResource as ResourceDependingOnFixed,
    )

    pipeline_ref = "tests.assets.pipelines.emit_one.counter.with_resource_depending_on_fixed"
    minion_ref = "tests.assets.minions.two_steps.counter.with_file_config"
    first_config = "tests/assets/config/minions/a.toml"
    second_config = "tests/assets/config/minions/b.toml"

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        fixed_resource_id = gru._get_resource_identity(FixedResource)
        depends_on_fixed_resource_id = gru._get_resource_identity(ResourceDependingOnFixed)

        first_start = await gru.start_orchestration(
            pipeline=pipeline_ref,
            minion=minion_ref,
            minion_config_path=first_config,
        )
        second_start = await gru.start_orchestration(
            pipeline=pipeline_ref,
            minion=minion_ref,
            minion_config_path=second_config,
        )
        assert first_start.success
        assert second_start.success
        assert first_start.orchestration_id is not None
        assert second_start.orchestration_id is not None
        assert first_start.orchestration_id != second_start.orchestration_id

        snapshot = gru.runtime_state_snapshot()
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

        assert_running_minions(
            gru,
            orchestration_to_minion_instance=running_orchestrations,
        )
        assert_pipeline_singleton(
            gru,
            pipeline_id=pipeline_ref,
            minion_instance_ids=set(running_orchestrations.values()),
        )
        assert_pipeline_resource_dependency_singletons(
            gru,
            pipeline_id=pipeline_ref,
            owner_resource_id=depends_on_fixed_resource_id,
            dependency_resource_id=fixed_resource_id,
            owner_refcount=1,
            dependency_refcount=1,
        )

        first_stop = await gru.stop_orchestration(first_start.orchestration_id)

        assert first_stop.success
        assert_running_minions(
            gru,
            orchestration_to_minion_instance={
                second_start.orchestration_id: second_minion_instance_id
            },
        )
        assert_pipeline_singleton(
            gru,
            pipeline_id=pipeline_ref,
            minion_instance_ids={second_minion_instance_id},
        )
        assert_pipeline_resource_dependency_singletons(
            gru,
            pipeline_id=pipeline_ref,
            owner_resource_id=depends_on_fixed_resource_id,
            dependency_resource_id=fixed_resource_id,
            owner_refcount=1,
            dependency_refcount=1,
        )

        second_stop = await gru.stop_orchestration(second_start.orchestration_id)

        assert second_stop.success
        assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_gru_injects_resource_dependencies_before_resource_startup(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as TwoStepCounterMinion,
    )
    from tests.assets.resources.fixed.default import AssetResource as FixedResource
    from tests.assets.resources.with_dependencies.depends_on_fixed import (
        AssetResource as ResourceDependingOnFixed,
    )

    TwoStepCounterMinion.enable_spy()
    TwoStepCounterMinion.reset_spy()

    pipeline_module_path = (
        "tests.assets.pipelines.emit_one.counter.with_resource_depending_on_fixed"
    )

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            minion="tests.assets.minions.two_steps.counter.default",
            pipeline=pipeline_module_path,
        )

        assert result.success
        await TwoStepCounterMinion.wait_for_calls(
            expected={"step_1": 1, "step_2": 1},
            timeout=5.0,
        )

        fixed_resource_id = gru._get_resource_identity(FixedResource)
        depends_on_fixed_resource_id = gru._get_resource_identity(ResourceDependingOnFixed)
        depends_on_fixed_resource_inst = gru._resources[depends_on_fixed_resource_id]
        assert gru._pipeline_resource_map[pipeline_module_path] == {depends_on_fixed_resource_id}
        assert gru._resource_dependencies[depends_on_fixed_resource_id] == {fixed_resource_id}
        assert gru._resource_reference_counts[depends_on_fixed_resource_id] == 1
        assert gru._resource_reference_counts[fixed_resource_id] == 1
        assert getattr(depends_on_fixed_resource_inst, "startup_value") == 123

        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert stop.success
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_starts_resource_with_multiple_resource_dependencies(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
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

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        result = await gru.start_orchestration(
            pipeline=CompoundDependencyPipeline,
            minion=MyMinion,
        )

        assert result.success

        first_id = gru._get_resource_identity(FirstLeafResource)
        second_id = gru._get_resource_identity(SecondLeafResource)
        compound_id = gru._get_resource_identity(MultiDependencyResource)

        compound = gru._resources[compound_id]
        assert isinstance(compound, MultiDependencyResource)
        assert compound.startup_value == 30
        assert compound.first is gru._resources[first_id]
        assert compound.second is gru._resources[second_id]
        assert gru._pipeline_resource_map[CompoundDependencyPipeline.__module__] == {compound_id}
        assert gru._resource_dependencies[compound_id] == {first_id, second_id}
        assert gru._resource_reference_counts[compound_id] == 1
        assert gru._resource_reference_counts[first_id] == 1
        assert gru._resource_reference_counts[second_id] == 1

        stop = await gru.stop_orchestration(result.orchestration_id or "")

        assert stop.success
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_start_fails_clearly_for_circular_resource_dependencies(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
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

    async with gru_factory(
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
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_allows_concurrent_starts_for_different_orchestrations_while_stop_is_in_flight(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        pipeline_b_ref = "tests.assets.pipelines.emit_one.simple.default_b"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        orchestration1_start_result = await gru.start_orchestration(
            pipeline=pipeline_ref,
            minion=minion_ref,
        )
        assert orchestration1_start_result.success

        orchestration1_stop_gate = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_stop_orchestration_minion", orchestration1_stop_gate)

        orchestration1_stop_task = asyncio.create_task(
            gru.stop_orchestration(orchestration1_start_result.orchestration_id or "")
        )
        await asyncio.wait_for(orchestration1_stop_gate.entered.wait(), timeout=1.0)

        orchestration2_start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_b_ref,
                minion=minion_ref,
            )
        )

        orchestration2_start_result = await asyncio.wait_for(orchestration2_start_task, timeout=1.0)
        assert orchestration2_start_result.success
        assert orchestration2_start_result.orchestration_id is not None
        assert_orchestration_running(gru, orchestration2_start_result.orchestration_id)
        orchestration2_id = Gru._make_orchestration_id(
            pipeline_id=gru._get_pipeline_identity_from_module_path(
                pipeline_b_ref,
            ),
            minion_id=gru._get_minion_identity_from_module_path(
                minion_ref,
            ),
            minion_config_id="",
        )
        assert_orchestration_running(gru, orchestration2_id)
        assert orchestration1_start_result.orchestration_id is not None
        assert_orchestration_running(gru, orchestration1_start_result.orchestration_id)
        assert_runtime_component_counts_exact(gru, minions=2)

        orchestration1_stop_gate.release.set()
        orchestration1_stop_result = await orchestration1_stop_task

        assert orchestration1_stop_result.success


@pytest.mark.asyncio
async def test_gru_shutdown_advertises_shutdown_before_waiting(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

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
async def test_gru_shutdown_waits_for_in_flight_start_orchestration(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gated_start_orchestration_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_orchestration_minion", gated_start_orchestration_minion)

        start_task = asyncio.create_task(
            gru.start_orchestration(
                minion="tests.assets.minions.two_steps.simple.default",
                pipeline="tests.assets.pipelines.emit_one.simple.default",
            )
        )

        await asyncio.wait_for(gated_start_orchestration_minion.entered.wait(), timeout=1.0)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task.done()

        gated_start_orchestration_minion.release.set()
        start_result, shutdown_result = await asyncio.gather(start_task, shutdown_task)

        assert start_result.success
        assert shutdown_result.success
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_shutdown_waits_for_in_flight_stop_orchestration(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        start_result = await gru.start_orchestration(
            minion=minion_ref,
            pipeline=pipeline_ref,
        )
        assert start_result.success

        gated_stop_orchestration_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_stop_orchestration_minion", gated_stop_orchestration_minion)

        stop_task = asyncio.create_task(gru.stop_orchestration(start_result.orchestration_id or ""))
        await asyncio.wait_for(gated_stop_orchestration_minion.entered.wait(), timeout=1.0)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task.done()

        gated_stop_orchestration_minion.release.set()
        stop_result, shutdown_result = await asyncio.gather(stop_task, shutdown_task)

        assert stop_result.success
        assert shutdown_result.success
        assert gru.runtime_state_snapshot().is_empty


@pytest.mark.asyncio
async def test_gru_shutdown_rejects_new_lifecycle_work_while_shutdown_is_pending(
    gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

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
                gru.start_orchestration(
                    minion="tests.assets.minions.two_steps.simple.default",
                    pipeline="tests.assets.pipelines.emit_one.simple.default",
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
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
        minion_ref = "tests.assets.minions.two_steps.simple.default"

        start_result = await gru.start_orchestration(
            pipeline=pipeline_ref,
            minion=minion_ref,
        )
        assert start_result.success

        gated__shutdown_async_component = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_shutdown_async_component", gated__shutdown_async_component)

        assert not gru._is_shutting_down
        shutdown_task = asyncio.create_task(gru.shutdown())
        await asyncio.wait_for(gated__shutdown_async_component.entered.wait(), timeout=1.0)

        new_start_task = asyncio.create_task(
            gru.start_orchestration(
                pipeline=pipeline_ref,
                minion=minion_ref,
            )
        )
        new_stop_task = asyncio.create_task(
            gru.stop_orchestration(start_result.orchestration_id or "")
        )

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
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:

    async with gru_factory(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    ) as gru:
        gated_start_orchestration_minion = GatedAsyncCallable[None]()
        monkeypatch.setattr(gru, "_start_orchestration_minion", gated_start_orchestration_minion)

        start_task = asyncio.create_task(
            gru.start_orchestration(
                minion="tests.assets.minions.two_steps.simple.default",
                pipeline="tests.assets.pipelines.emit_one.simple.default",
            )
        )

        await asyncio.wait_for(gated_start_orchestration_minion.entered.wait(), timeout=1.0)

        assert not gru._is_shutting_down
        shutdown_task_1 = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert gru._is_shutting_down
        assert not shutdown_task_1.done()

        shutdown_task_2 = asyncio.create_task(gru.shutdown())
        await asyncio.sleep(0)
        assert not shutdown_task_2.done()

        gated_start_orchestration_minion.release.set()
        start_result, shutdown_result_1, shutdown_result_2 = await asyncio.gather(
            start_task,
            shutdown_task_1,
            shutdown_task_2,
        )

        assert start_result.success
        assert shutdown_result_1.success
        assert shutdown_result_2.success
        assert gru.runtime_state_snapshot().is_empty
