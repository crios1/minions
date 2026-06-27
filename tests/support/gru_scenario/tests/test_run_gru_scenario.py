from pathlib import Path

import pytest

from minions._internal._domain.component_identity import get_component_id
from minions._internal._domain.gru import Gru
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.support.gru_scenario import (
    AfterWorkflowStepStarts,
    Concurrent,
    Directive,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
    run_gru_scenario,
)


@pytest.mark.asyncio
async def test_run_gru_scenario_uses_durable_pipeline_id_for_event_targets(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.pipelines.emit_one.counter.identified import (
        AssetPipeline as IdentifiedEmitOneCounterPipeline,
    )

    pipeline_id = get_component_id(IdentifiedEmitOneCounterPipeline)
    assert pipeline_id is not None

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.identified",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_id: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_with_new_assets(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_two.counter.after_two_subscribers"
    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.counter.default",
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.counter.with_fixed_resource",
    )

    directives: list[Directive] = [
        Concurrent(
            start_1,
            start_2,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id=start_1, expect_success=True),
        OrchestrationStop(id=start_2, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 2},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_helper_basic(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion="tests.assets.minions.two_steps.simple.default",
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_accepts_class_start_with_inline_minion_config(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.minions.two_steps.simple.with_config import (
        AssetMinion as ConfiguredMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as EmitOneSimplePipeline,
    )
    from tests.assets.support.minion_spied_configed import AssetMinionConfig

    start = OrchestrationStart(
        pipeline=EmitOneSimplePipeline,
        minion=ConfiguredMinion,
        minion_config=AssetMinionConfig(name="inline"),
    )

    directives: list[Directive] = [
        start,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={start: {"succeeded": 1, "failed": 0, "aborted": 0}},
                workflow_steps={start: {"step_1": 1, "step_2": 1}},
                workflow_steps_mode="exact",
            ),
        ),
        OrchestrationStop(id=start, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={EmitOneSimplePipeline: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflow_step_starts_then_stop_happy_path(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.failure.abort_step"
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=minion_ref,
    )

    directives: list[Directive] = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_persistence_after_stop(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.failure.slow_step"
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=minion_ref,
    )

    directives: list[Directive] = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(persistence={start: 1}),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_resolutions_after_completion(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.two_steps.simple.default"
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"

    start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    directives: list[Directive] = [
        start,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={start: {"succeeded": 1, "failed": 0, "aborted": 0}}
            ),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_workflow_steps_exact_after_completion(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.two_steps.simple.default"
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"

    start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    directives: list[Directive] = [
        start,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                workflow_steps={start: {"step_1": 1, "step_2": 1}},
                workflow_steps_mode="exact",
            ),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_mixed_wait_workflow_step_modes_end_to_end(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref_a = "tests.assets.minions.two_steps.simple.default"
    minion_ref_b = "tests.assets.minions.two_steps.simple.with_simple_b_resource"
    pipeline_ref_a = "tests.assets.pipelines.emit_one.simple.default"
    pipeline_ref_b = "tests.assets.pipelines.emit_one.simple.default_b"
    start_a = OrchestrationStart(pipeline=pipeline_ref_a, minion=minion_ref_a)
    start_b = OrchestrationStart(pipeline=pipeline_ref_b, minion=minion_ref_b)

    directives: list[Directive] = [
        start_a,
        # Intentional tolerance window for mixed-mode end-to-end coverage.
        WaitWorkflowCompletions(workflow_steps_mode="at_least"),
        start_b,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={
                    start_a: {"succeeded": 1, "failed": 0, "aborted": 0},
                    start_b: {"succeeded": 1, "failed": 0, "aborted": 0},
                },
                workflow_steps={
                    start_a: {"step_1": 1, "step_2": 1},
                    start_b: {"step_1": 1, "step_2": 1},
                },
                workflow_steps_mode="exact",
            ),
        ),
        OrchestrationStop(id=start_a, expect_success=True),
        OrchestrationStop(id=start_b, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={
            pipeline_ref_a: 1,
            pipeline_ref_b: 1,
        },
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_at_checkpoint_index(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.failure.slow_step"
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)

    directives: list[Directive] = [
        start,
        AfterWorkflowStepStarts(
            expected={start: {"step_1": 1}},
            directive=OrchestrationStop(id=start, expect_success=True),
        ),
        ExpectRuntime(
            at=0,
            expect=RuntimeExpectSpec(persistence={start: 1}),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_restart_same_pipeline_with_persistence_and_resolutions(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.failure.slow_step"
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    start_1 = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    start_2 = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)

    directives: list[Directive] = [
        start_1,
        AfterWorkflowStepStarts(
            expected={start_1: {"step_1": 1}},
            directive=OrchestrationStop(id=start_1, expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                persistence={start_1: 1},
                workflow_steps={start_1: {"step_1": 1}},
                workflow_steps_mode="exact",
            ),
        ),
        start_2,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={start_2: {"succeeded": 2, "failed": 0, "aborted": 0}},
                workflow_steps={start_2: {"step_1": 2}},
                workflow_steps_mode="exact",
            ),
        ),
        OrchestrationStop(id=start_2, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_resume_from_explicit_step_boundary_does_not_replay_completed_steps(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.two_steps.counter.slow_second_step"
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    first_start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    second_start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)

    directives: list[Directive] = [
        first_start,
        AfterWorkflowStepStarts(
            expected={first_start: {"step_2": 1}},
            directive=OrchestrationStop(id=first_start, expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                persistence={first_start: 1},
                workflow_steps={first_start: {"step_1": 1, "step_2": 1}},
                workflow_steps_mode="exact",
            ),
        ),
        second_start,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={second_start: {"succeeded": 2, "failed": 0, "aborted": 0}},
                workflow_steps={second_start: {"step_1": 2, "step_2": 2}},
                workflow_steps_mode="exact",
            ),
        ),
        OrchestrationStop(id=second_start, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_resume_identified_minion_without_persisted_minion_metadata(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    from tests.assets.pipelines.emit_one.counter.identified import (
        AssetPipeline as IdentifiedEmitOneCounterPipeline,
    )

    pipeline_id = get_component_id(IdentifiedEmitOneCounterPipeline)
    assert pipeline_id is not None

    minion_ref = (
        "tests.assets.minions.two_steps.counter."
        "identified_with_fixed_resource_slow_second_step"
    )

    pipeline_ref = "tests.assets.pipelines.emit_one.counter.identified"
    first_start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    second_start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)

    directives: list[Directive] = [
        first_start,
        AfterWorkflowStepStarts(
            expected={first_start: {"step_2": 1}},
            directive=OrchestrationStop(id=first_start, expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                persistence={first_start: 1},
                workflow_steps={
                    first_start: {"step_1": 1, "step_2": 1},
                },
                workflow_steps_mode="exact",
            ),
        ),
        second_start,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={
                    second_start: {
                        "succeeded": 2,
                        "failed": 0,
                        "aborted": 0,
                    },
                },
                workflow_steps={
                    second_start: {"step_1": 2, "step_2": 2},
                },
                workflow_steps_mode="exact",
            ),
        ),
        OrchestrationStop(id=second_start, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_id: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_batches_stops_serial(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default"
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.with_simple_b_resource"
    )

    directives: list[Directive] = [
        start_1,
        start_2,
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id=start_1, expect_success=True),
        OrchestrationStop(id=start_2, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_duplicate_start_fails(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    minion_ref = "tests.assets.minions.two_steps.simple.default"

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
        ),
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            expect_success=False,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_failed_start_does_not_require_minion_startup(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.record.default"
    minion_ref = "tests.assets.minions.two_steps.simple.default"

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            expect_success=False,
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_stop_unknown_fails(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion="tests.assets.minions.two_steps.simple.default",
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id="missing-orchestration", expect_success=False),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_parallel_starts(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default"
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.with_simple_b_resource"
    )

    directives: list[Directive] = [
        Concurrent(start_1, start_2),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id=start_1, expect_success=True),
        OrchestrationStop(id=start_2, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflows_subset(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default"
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.with_simple_b_resource"
    )

    directives: list[Directive] = [
        start_1,
        start_2,
        WaitWorkflowCompletions(orchestrations=(start_1,), workflow_steps_mode="exact"),
        OrchestrationStop(id=start_1, expect_success=True),
        OrchestrationStop(id=start_2, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflows_unknown_start_fails(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=1)

    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default",
    )
    start_missing = OrchestrationStart(pipeline=pipeline_ref, minion="missing-minion")
    directives: list[Directive] = [
        start,
        WaitWorkflowCompletions(orchestrations=(start_missing,)),
        GruShutdown(expect_success=True),
    ]

    with pytest.raises(ValueError, match="outside this ScenarioPlan"):
        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_ref: 1},
        )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_exact_reports_mismatch(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=2)

    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default",
    )
    start_other = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.with_simple_b_resource",
    )

    directives: list[Directive] = [
        Concurrent(
            start,
            start_other,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                workflow_steps={start: {"step_1": 0, "step_2": 0}},
                workflow_steps_mode="exact",
            ),
        ),
        GruShutdown(expect_success=True),
    ]

    with pytest.raises(
        pytest.fail.Exception,
        match=(
            r"ExpectRuntime\.workflow_steps mismatch for "
            r"start 0\.step_1: expected 0, got 1"
        ),
    ):
        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_ref: 1},
        )


@pytest.mark.asyncio
async def test_run_gru_scenario_strict_wait_workflow_window_overlap_mismatch(
    gru: Gru,
    tests_dir: Path,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_ref = (
        "tests.assets.pipelines.emit_two.simple.with_subscriber_counts_one_then_two"
    )
    minion_ref = "tests.assets.minions.two_steps.simple.with_config"

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            minion_config_path=cfg1,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            minion_config_path=cfg2,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    # This is the e2e lock for strict-window mismatch diagnostics:
    # workflow-id mismatch must surface first, and include bounded call-count context.
    with pytest.raises(
        pytest.fail.Exception,
        match=(
            r"Checkpoint workflow-id progression mismatch.*expected workflow-id "
            r"delta 1, got 2\..*Call-count delta: 2 \(expected 1\.\.2\)"
        ),
    ):
        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_ref: 1},
        )


@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflows_empty_is_noop(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default"
    )

    directives: list[Directive] = [
        start,
        # Intentional no-op path for empty subset handling.
        WaitWorkflowCompletions(orchestrations=()),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id=start, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_parallel_mixed_directives(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default"
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.with_simple_b_resource"
    )

    directives: list[Directive] = [
        Concurrent(start_1, start_2),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        Concurrent(
            WaitWorkflowCompletions(orchestrations=()),
            OrchestrationStop(id=start_2, expect_success=True),
        ),
        OrchestrationStop(id=start_1, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_simple_start_wait_shutdown(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion="tests.assets.minions.two_steps.simple.default",
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_dsl_exploration(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.default"
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion="tests.assets.minions.two_steps.simple.with_simple_b_resource"
    )

    directives: list[Directive] = [
        Concurrent(start_1, start_2),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        OrchestrationStop(id=start_1, expect_success=True),
        OrchestrationStop(id=start_2, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_golden_regression_mixed_concurrent_wait_subset(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_ref = "tests.assets.minions.two_steps.simple.default"
    pipeline_ref = "tests.assets.pipelines.emit_one.simple.default"
    from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

    AssetPipeline.configure_gate(expected_subs=1)

    start_1 = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)

    directives: list[Directive] = [
        Concurrent(
            start_1,
            OrchestrationStart(
                pipeline=pipeline_ref,
                minion=minion_ref,
                expect_success=False,
            ),
        ),
        WaitWorkflowCompletions(orchestrations=(start_1,), workflow_steps_mode="exact"),
        OrchestrationStop(id=start_1, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )
