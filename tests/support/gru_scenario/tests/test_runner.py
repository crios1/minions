import asyncio
from collections.abc import Callable

import pytest

from minions._internal._domain.component_identity import get_component_id
from minions._internal._domain.gru import Gru, GruRuntimeStateSnapshot
from tests.support.gru_scenario.directives import (
    AfterWorkflowStepStarts,
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
)
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import (
    OrchestrationStartReceipt,
    ScenarioRunner,
    ScenarioRunResult,
    ScenarioWaiter,
    SpyRegistry,
)


def test_orchestration_start_receipt_requires_durable_ids() -> None:
    with pytest.raises(ValueError, match="pipeline_id is required"):
        OrchestrationStartReceipt(
            0,
            "tests.assets.minions.two_steps.simple.default",
            "tests.assets.pipelines.emit_one.simple.default",
            None,
            None,
            False,
            "mock-orchestration-id",
            minion_id="tests.assets.minions.two_steps.simple.default",
        )

    with pytest.raises(ValueError, match="minion_id is required"):
        OrchestrationStartReceipt(
            0,
            "tests.assets.minions.two_steps.simple.default",
            "tests.assets.pipelines.emit_one.simple.default",
            None,
            None,
            False,
            "mock-orchestration-id",
            pipeline_id="tests.assets.pipelines.emit_one.simple.default",
        )


@pytest.mark.asyncio
async def test_runner_require_result_invariant_message_is_actionable(gru: Gru) -> None:
    runner = ScenarioRunner(
        gru,
        ScenarioPlan([], pipeline_event_counts={}),
        per_verification_timeout=0.1,
    )
    with pytest.raises(
        AssertionError,
        match=r"internal invariant violated: _result is None.*ScenarioRunner\.run\(\)",
    ):
        runner._require_result()


@pytest.mark.asyncio
async def test_runner_validates_missing_pipeline_event_count_after_resolving_identity(
    gru: Gru,
) -> None:
    plan = ScenarioPlan(
        [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.simple.default",
                minion="tests.assets.minions.two_steps.simple.default",
            )
        ],
        pipeline_event_counts={},
    )

    with pytest.raises(pytest.fail.Exception, match="Missing pipeline_event_counts entries"):
        await ScenarioRunner(gru, plan, per_verification_timeout=0.1).run()


@pytest.mark.asyncio
async def test_runner_validates_unused_pipeline_event_count_after_resolving_identity(
    gru: Gru,
) -> None:
    plan = ScenarioPlan(
        [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.simple.default",
                minion="tests.assets.minions.two_steps.simple.default",
            )
        ],
        pipeline_event_counts={
            "tests.assets.pipelines.emit_one.simple.default": 1,
            "unused-pipeline-id": 1,
        },
    )

    with pytest.raises(pytest.fail.Exception, match="pipelines not started in directives"):
        await ScenarioRunner(gru, plan, per_verification_timeout=0.1).run()


@pytest.mark.asyncio
async def test_runner_records_receipts_for_success_and_expected_failure(
    gru: Gru,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    pipeline_id = pipeline_ref

    directives = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion="tests.assets.minions.two_steps.simple.default",
        ),
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion="tests.assets.minions.two_steps.simple.default",
            expect_success=False,
        ),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_id: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 2
    assert result.receipts[0].directive_index == 0
    assert result.receipts[0].success is True
    assert result.receipts[0].instance_id is not None

    assert result.receipts[1].directive_index == 1
    assert result.receipts[1].success is False

    assert result.seen_shutdown is True


@pytest.mark.asyncio
async def test_runner_concurrent_starts_capture_started_minions_and_instance_tags(
    gru: Gru,
    configure_emit_one_simple_pipeline_subscriber_gate: Callable[..., None],
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    pipeline_id = pipeline_ref
    configure_emit_one_simple_pipeline_subscriber_gate(expected_subs=2)

    directives = [
        Concurrent(
            OrchestrationStart(
                pipeline=pipeline_ref,
                minion="tests.assets.minions.two_steps.simple.default",
            ),
            OrchestrationStart(
                pipeline=pipeline_ref,
                minion="tests.assets.minions.two_steps.simple.with_simple_b_resource",
            ),
        ),
        WaitWorkflowCompletions(),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_id: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 2
    assert all(r.success for r in result.receipts)
    assert {r.directive_index for r in result.receipts} == {0, 1}
    assert len(result.started_minions) == 2

    started_locations = {m._mn_minion_module_path for m in result.started_minions}
    assert started_locations == {
        "tests.assets.minions.two_steps.simple.default",
        "tests.assets.minions.two_steps.simple.with_simple_b_resource",
    }

    assert result.spies is not None
    pipeline_cls = result.spies.pipelines[pipeline_id]
    assert len(result.instance_tags.get(pipeline_cls, set())) >= 1

    assert any(len(result.instance_tags.get(type(m), set())) >= 1 for m in result.started_minions)
    assert any(len(result.instance_tags.get(r_cls, set())) >= 1 for r_cls in result.spies.resources)


@pytest.mark.asyncio
async def test_runner_tracks_durable_minion_pipeline_and_resources(
    gru: Gru,
) -> None:
    from tests.assets.minions.two_steps.counter.identified_with_fixed_resource import (
        AssetMinion as IdentifiedFixedResourceMinion,
    )
    from tests.assets.pipelines.emit_one.counter.identified import (
        AssetPipeline as IdentifiedEmitOneCounterPipeline,
    )
    from tests.assets.resources.fixed.identified import (
        AssetResource as IdentifiedFixedResource,
    )

    minion_id = get_component_id(IdentifiedFixedResourceMinion)
    pipeline_id = get_component_id(IdentifiedEmitOneCounterPipeline)
    resource_id = get_component_id(IdentifiedFixedResource)
    assert minion_id is not None
    assert pipeline_id is not None
    assert resource_id is not None

    start = OrchestrationStart(
        pipeline="tests.assets.pipelines.emit_one.counter.identified",
        minion="tests.assets.minions.two_steps.counter.identified_with_fixed_resource",
    )
    plan = ScenarioPlan(
        [start, WaitWorkflowCompletions(workflow_steps_mode="exact")],
        pipeline_event_counts={pipeline_id: 1},
    )

    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 1
    receipt = result.receipts[0]
    assert receipt.success
    assert receipt.minion_id == minion_id
    assert receipt.pipeline_id == pipeline_id
    assert receipt.orchestration_id is not None
    assert receipt.orchestration_id in gru._minions_by_orchestration_id
    assert pipeline_id in gru._pipelines
    assert resource_id in gru._resources

    minion = gru._minions_by_orchestration_id[receipt.orchestration_id]
    assert minion._mn_minion_id == minion_id
    assert gru._minion_pipeline_map[minion._mn_minion_instance_id] == pipeline_id
    assert gru._minion_resource_map[minion._mn_minion_instance_id] == {resource_id}

    assert result.spies is not None
    assert (
        result.spies.pipelines[pipeline_id].__name__ == "AssetPipeline"
    )
    assert result.spies.minions[minion_id] is IdentifiedFixedResourceMinion
    assert IdentifiedFixedResource in result.spies.resources

    shutdown = await gru.shutdown()
    assert shutdown.success


@pytest.mark.asyncio
async def test_runner_wait_workflows_subset_handles_mixed_success_and_failure(
    gru: Gru,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    pipeline_id = pipeline_ref

    successful_start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default",
    )
    failed_start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default",
        expect_success=False,
    )
    directives = [
        successful_start,
        failed_start,
        WaitWorkflowCompletions(orchestrations=(successful_start,)),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_id: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 2
    assert sum(1 for r in result.receipts if r.success) == 1
    assert sum(1 for r in result.receipts if not r.success) == 1
    assert len(result.started_minions) == 1


@pytest.mark.asyncio
async def test_wait_minion_tasks_times_out_instead_of_blocking_indefinitely(
    gru: Gru,
) -> None:
    class _DummyMinion:
        def __init__(self) -> None:
            self._mn_tasks_gate = asyncio.Lock()
            self._mn_service_tasks: set[asyncio.Task[None]] = set()
            self._mn_workflow_tasks: set[asyncio.Task[None]] = set()

        async def _mn_wait_until_all_tasks_idle(self, timeout: float = 2.0) -> None:
            deadline = asyncio.get_running_loop().time() + timeout
            while True:
                async with self._mn_tasks_gate:
                    tasks = tuple(self._mn_workflow_tasks | self._mn_service_tasks)
                if not tasks:
                    return
                remaining = deadline - asyncio.get_running_loop().time()
                if remaining <= 0:
                    raise TimeoutError("dummy minion tasks did not become idle")
                done, pending = await asyncio.wait(tasks, timeout=remaining)
                if pending and not done:
                    raise TimeoutError("dummy minion tasks did not become idle")

    plan = ScenarioPlan([], pipeline_event_counts={})
    waiter = ScenarioWaiter(
        plan,
        ScenarioRunner(gru, plan, per_verification_timeout=0.01)._insp,
        timeout=0.01,
        spies=SpyRegistry(),
        result=ScenarioRunResult(),
    )

    dummy = _DummyMinion()
    task = asyncio.create_task(asyncio.sleep(60), name="never-finishes")
    dummy._mn_workflow_tasks.add(task)

    try:
        with pytest.raises(pytest.fail.Exception, match="WaitWorkflowCompletions timed out"):
            await waiter._wait_minion_tasks({dummy})  # type: ignore[arg-type]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_wait_minion_tasks_waits_for_minions_concurrently(gru: Gru) -> None:
    class _DummyMinion:
        def __init__(
            self,
            started_event: asyncio.Event,
            peer_started_event: asyncio.Event,
        ) -> None:
            self._mn_tasks_gate = asyncio.Lock()
            self._mn_service_tasks: set[asyncio.Task[None]] = set()
            self._mn_workflow_tasks: set[asyncio.Task[None]] = set()
            self._started_event = started_event
            self._peer_started_event = peer_started_event

        async def _mn_wait_until_all_tasks_idle(self, timeout: float = 2.0) -> None:
            self._started_event.set()
            await asyncio.wait_for(self._peer_started_event.wait(), timeout=timeout)

    plan = ScenarioPlan([], pipeline_event_counts={})
    waiter = ScenarioWaiter(
        plan,
        ScenarioRunner(gru, plan, per_verification_timeout=0.05)._insp,
        timeout=0.05,
        spies=SpyRegistry(),
        result=ScenarioRunResult(),
    )

    first_started = asyncio.Event()
    second_started = asyncio.Event()
    first = _DummyMinion(first_started, second_started)
    second = _DummyMinion(second_started, first_started)

    await waiter._wait_minion_tasks({first, second})  # type: ignore[arg-type]


@pytest.mark.asyncio
@pytest.mark.asyncio
async def test_runner_wait_workflow_step_starts_then_rejects_unsupported_wrapped_directive(
    gru: Gru,
) -> None:
    start = OrchestrationStart(
        pipeline="tests.assets.pipelines.emit_one.simple.default",
        minion="tests.assets.minions.two_steps.simple.default",
    )
    directives = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=GruShutdown(expect_success=True),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={
            "tests.assets.pipelines.emit_one.simple.default": 1
        },
    )
    runner = ScenarioRunner(gru, plan, per_verification_timeout=0.1)

    with pytest.raises(pytest.fail.Exception, match="supports wrapping OrchestrationStop only"):
        await runner.run()


@pytest.mark.asyncio
async def test_runner_wait_workflow_step_starts_then_rejects_external_start(gru: Gru) -> None:
    start = OrchestrationStart(pipeline="missing", minion="missing")
    directives = [
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id="missing", expect_success=False),
        ),
    ]
    with pytest.raises(ValueError, match="outside this ScenarioPlan"):
        ScenarioPlan(directives, pipeline_event_counts={})


@pytest.mark.asyncio
async def test_runner_wait_workflow_step_starts_then_rejects_unknown_steps(gru: Gru) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    pipeline_id = pipeline_ref
    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default",
    )
    directives = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"missing_step": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
    ]
    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_id: 1})
    runner = ScenarioRunner(gru, plan, per_verification_timeout=0.1)

    with pytest.raises(
        pytest.fail.Exception,
        match="Unknown workflow step in AfterWorkflowStepStarts.expected"
    ):
        await runner.run()


@pytest.mark.asyncio
async def test_runner_records_checkpoints_for_wait_workflow_completions_and_shutdown(
    gru: Gru,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    pipeline_id = pipeline_ref

    directives = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion="tests.assets.minions.two_steps.simple.default",
        ),
        WaitWorkflowCompletions(),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_id: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert [cp.kind for cp in result.checkpoints] == [
        "wait_workflow_completions",
        "gru_shutdown",
    ]
    assert [cp.order for cp in result.checkpoints] == [0, 1]
    assert result.checkpoints[0].directive_type == "WaitWorkflowCompletions"
    assert result.checkpoints[0].orchestration_directive_indexes is None
    assert result.checkpoints[0].workflow_steps_mode == "at_least"
    assert result.checkpoints[0].spy_call_counts_by_instance is not None
    assert result.checkpoints[0].workflow_step_started_ids_by_minion_id is not None
    assert result.checkpoints[1].directive_type == "GruShutdown"
    assert result.checkpoints[1].seen_shutdown is True


@pytest.mark.asyncio
async def test_runner_records_lifecycle_observations_after_start_stop_and_shutdown(
    gru: Gru,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.counter.default",
    )
    directives = [
        start,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id=start, expect_success=True),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )

    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert [
        observation.directive_type for observation in result.lifecycle_observations
    ] == [
        OrchestrationStart,
        OrchestrationStop,
        GruShutdown,
    ]
    start_observation, stop_observation, shutdown_observation = (
        result.lifecycle_observations
    )
    receipt = result.receipts[0]
    assert start_observation.gru_runtime_state.orchestrations == {
        receipt.orchestration_id
    }
    assert stop_observation.active_orchestration_start_indexes == frozenset()
    empty_runtime = GruRuntimeStateSnapshot(
        minion_instances=frozenset(),
        orchestrations=frozenset(),
        minion_tasks=frozenset(),
        pipelines=frozenset(),
        pipeline_tasks=frozenset(),
        resources=frozenset(),
        resource_tasks=frozenset(),
        pipeline_by_minion_instance={},
        resources_by_minion_instance={},
        resources_by_pipeline={},
        resource_dependencies_by_dependent_resource={},
        resource_dependents_by_dependency_resource={},
        resource_reference_counts={},
    )
    assert stop_observation.gru_runtime_state == empty_runtime
    assert shutdown_observation.seen_shutdown
    assert shutdown_observation.gru_runtime_state == empty_runtime


@pytest.mark.asyncio
async def test_runner_records_checkpoint_for_wait_workflow_step_starts_then(gru: Gru) -> None:
    start = OrchestrationStart(
        pipeline="tests.assets.pipelines.emit_one.counter.default",
        minion="tests.assets.minions.failure.abort_step",
    )
    directives = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    checkpoints = result.checkpoints
    assert [cp.kind for cp in checkpoints] == ["wait_workflow_step_starts_then", "gru_shutdown"]
    assert checkpoints[0].directive_type == "AfterWorkflowStepStarts"
    assert checkpoints[0].expected_step_starts == {0: {"step_1": 1}}
    assert checkpoints[0].wrapped_directive_type == "OrchestrationStop"


@pytest.mark.asyncio
async def test_runner_restart_flow_checkpoints_separate_pre_stop_and_post_restart_windows(
    gru: Gru,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    pipeline_id = pipeline_ref
    minion_ref = "tests.assets.minions.two_steps.counter.default"
    minion_id = minion_ref
    start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    directives = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref),
        WaitWorkflowCompletions(),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={pipeline_id: 1},
    )
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    checkpoints = result.checkpoints
    assert [cp.kind for cp in checkpoints] == [
        "wait_workflow_step_starts_then",
        "wait_workflow_completions",
        "gru_shutdown",
    ]

    pre_stop_cp = checkpoints[0]
    post_restart_cp = checkpoints[1]

    assert pre_stop_cp.receipt_count == 1
    assert post_restart_cp.receipt_count == 2
    assert post_restart_cp.workflow_steps_mode == "at_least"

    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_ref = "tests.assets.minions.two_steps.counter.default"
    minion_id = minion_ref
    assert pre_stop_cp.spy_call_counts is not None
    assert pre_stop_cp.spy_call_counts_by_instance is not None
    assert pre_stop_cp.workflow_step_started_ids_by_minion_id is not None
    assert post_restart_cp.spy_call_counts is not None
    assert post_restart_cp.spy_call_counts_by_instance is not None
    assert post_restart_cp.workflow_step_started_ids_by_minion_id is not None

    pre_step1 = pre_stop_cp.spy_call_counts.get(class_key, {}).get("step_1", 0)
    post_step1 = post_restart_cp.spy_call_counts.get(class_key, {}).get("step_1", 0)
    assert post_step1 >= pre_step1 + 1
    pre_by_instance = pre_stop_cp.spy_call_counts_by_instance.get(class_key, {})
    post_by_instance = post_restart_cp.spy_call_counts_by_instance.get(class_key, {})
    assert len(pre_by_instance) >= 1
    assert len(post_by_instance) >= 1
    pre_workflows = pre_stop_cp.workflow_step_started_ids_by_minion_id.get(minion_id, {}).get(
        "step_1", ()
    )
    post_workflows = post_restart_cp.workflow_step_started_ids_by_minion_id.get(minion_id, {}).get(
        "step_1", ()
    )
    assert len(post_workflows) >= len(pre_workflows)


@pytest.mark.asyncio
async def test_runner_restart_same_orchestration_id_after_stop_succeeds(gru: Gru) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    pipeline_id = pipeline_ref
    minion_ref = "tests.assets.minions.failure.abort_step"
    start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)

    directives = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref, expect_success=True),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={pipeline_id: 1},
    )
    await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()


@pytest.mark.asyncio
async def test_runner_records_expect_runtime_checkpoint_with_persistence_snapshot(
    gru: Gru,
) -> None:
    from tests.assets.minions.failure.slow_step import AssetMinion as SlowStepMinion

    minion_id = get_component_id(SlowStepMinion)
    assert minion_id is not None

    start = OrchestrationStart(
        pipeline="tests.assets.pipelines.emit_one.counter.default",
        minion="tests.assets.minions.failure.slow_step",
    )
    directives = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        ExpectRuntime(expect=RuntimeExpectSpec(persistence={start: 1})),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    expect_cps = [cp for cp in result.checkpoints if cp.kind == "expect_runtime"]
    assert len(expect_cps) == 1
    persisted = expect_cps[0].persisted_contexts_by_minion_id
    assert persisted is not None
    assert persisted.get(minion_id, 0) >= 1
    assert "tests.assets.minions.failure.slow_step" not in persisted
    assert expect_cps[0].spy_call_counts_by_instance is not None
    assert expect_cps[0].workflow_step_started_ids_by_minion_id is not None
