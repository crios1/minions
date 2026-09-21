from pathlib import Path

import pytest

from minions._internal._domain.component_identity import get_component_id
from minions._internal._domain.gru import Gru
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
async def test_uses_durable_pipeline_id_for_event_targets(
    gru: Gru,
):
    from tests.assets.minions.two_steps.counter.default import (
        AssetMinion as CounterMinion,
    )
    from tests.assets.pipelines.emit_one.counter.identified import (
        AssetPipeline as IdentifiedEmitOneCounterPipeline,
    )

    assert get_component_id(IdentifiedEmitOneCounterPipeline) is not None

    directives: list[Directive] = [
        OrchestrationStart(
            pipeline=IdentifiedEmitOneCounterPipeline.__module__,
            minion=CounterMinion.__module__,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        directives,
        pipeline_event_counts={IdentifiedEmitOneCounterPipeline: 1},
    )


@pytest.mark.asyncio
async def test_supports_start_wait_shutdown_flow(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )

@pytest.mark.asyncio
async def test_class_start_with_inline_config_records_successful_resolution_and_exact_steps(
    gru: Gru,
):
    from tests.assets.minions.two_steps.simple.with_config import (
        AssetMinion as ConfiguredSimpleMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as EmitOneSimplePipeline,
    )
    from tests.assets.support.minion_spied_configed import AssetMinionConfig

    start = OrchestrationStart(
        pipeline=EmitOneSimplePipeline,
        minion=ConfiguredSimpleMinion,
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
        directives,
        pipeline_event_counts={EmitOneSimplePipeline: 1},
    )


@pytest.mark.asyncio
async def test_supports_after_workflow_step_starts_wrapping_orchestration_stop(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_after_workflow_step_starts_scopes_wait_to_referenced_start_directive(
    gru: Gru,
):
    minion_ref = "tests.assets.minions.failure.slow_step"
    first_pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    second_pipeline_ref = "tests.assets.pipelines.emit_one.counter.default_b"
    first_start = OrchestrationStart(pipeline=first_pipeline_ref, minion=minion_ref)
    # Use a distinct pipeline identity so the same minion class can run in two
    # orchestrations without triggering duplicate-start rejection.
    second_start = OrchestrationStart(pipeline=second_pipeline_ref, minion=minion_ref)

    directives: list[Directive] = [
        first_start,
        # Complete the first instance so its class-level step count could
        # incorrectly satisfy the later instance's step boundary.
        WaitWorkflowCompletions(
            orchestrations=(first_start,),
            workflow_steps_mode="exact",
        ),
        second_start,
        AfterWorkflowStepStarts(
            expected={second_start: {"step_1": 1}},
            directive=OrchestrationStop(id=second_start, expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                workflow_steps={second_start: {"step_1": 1}},
                workflow_steps_mode="exact",
            ),
        ),
        OrchestrationStop(id=first_start, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        directives,
        pipeline_event_counts={first_pipeline_ref: 1, second_pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_supports_expect_runtime_for_persistence_after_stop(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_supports_mixed_wait_workflow_step_modes_end_to_end(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={
            pipeline_ref_a: 1,
            pipeline_ref_b: 1,
        },
    )


@pytest.mark.asyncio
async def test_supports_expect_runtime_at_checkpoint_index(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_supports_mixed_directives_in_concurrent_group(
    gru: Gru,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.minions.two_steps.simple.with_simple_b_resource import (
        AssetMinion as SimpleResourceBMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as EmitOneSimplePipeline,
    )

    pipeline_ref = EmitOneSimplePipeline.__module__
    EmitOneSimplePipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleMinion.__module__,
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleResourceBMinion.__module__,
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_restart_same_pipeline_preserves_persistence_and_resolutions(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_explicit_step_boundary_resume_excludes_completed_step_replay(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_resumes_identified_minion_without_persisted_minion_metadata(
    gru: Gru,
):
    from tests.assets.minions.two_steps.counter import (
        identified_with_fixed_resource_slow_second_step as SlowSecondStepMinionModule,
    )
    from tests.assets.pipelines.emit_one.counter.identified import (
        AssetPipeline as IdentifiedEmitOneCounterPipeline,
    )

    pipeline_id = get_component_id(IdentifiedEmitOneCounterPipeline)
    assert pipeline_id is not None

    minion_ref = SlowSecondStepMinionModule.AssetMinion.__module__
    pipeline_ref = IdentifiedEmitOneCounterPipeline.__module__
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
        directives,
        pipeline_event_counts={pipeline_id: 1},
    )


@pytest.mark.asyncio
async def test_event_type_mismatch_rejects_start_before_minion_construction(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={},
    )


@pytest.mark.asyncio
async def test_unknown_stop_fails(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_runs_starts_in_parallel(
    gru: Gru,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.minions.two_steps.simple.with_simple_b_resource import (
        AssetMinion as SimpleResourceBMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as EmitOneSimplePipeline,
    )

    pipeline_ref = EmitOneSimplePipeline.__module__
    EmitOneSimplePipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleMinion.__module__,
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleResourceBMinion.__module__,
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_wait_workflow_completions_targets_selected_orchestrations(
    gru: Gru,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.minions.two_steps.simple.with_simple_b_resource import (
        AssetMinion as SimpleResourceBMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as EmitOneSimplePipeline,
    )

    pipeline_ref = EmitOneSimplePipeline.__module__
    EmitOneSimplePipeline.configure_gate(expected_subs=2)

    start_1 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleMinion.__module__,
    )
    start_2 = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleResourceBMinion.__module__,
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )


@pytest.mark.asyncio
async def test_wait_workflow_completions_does_not_wait_on_unselected_same_class_start(
    gru: Gru,
):
    import asyncio
    from dataclasses import dataclass

    from minions import minion_step
    from tests.assets.contexts.counter import CounterContext
    from tests.assets.events.counter import CounterEvent
    from tests.assets.pipelines.emit_one.counter.default import (
        AssetPipeline as ReadyPipeline,
    )
    from tests.assets.pipelines.emit_one.counter.default_b import (
        AssetPipeline as BlockedPipeline,
    )
    from tests.assets.support.minion_spied import SpiedMinion

    @dataclass
    class MinionConfig:
        stall: bool

    class SameClassMinion(SpiedMinion[CounterEvent, CounterContext]):
        config: MinionConfig

        @minion_step
        async def step_1(self) -> None:
            if self.config.stall:
                await asyncio.Event().wait()

    selected_start = OrchestrationStart(
        pipeline=ReadyPipeline,
        minion=SameClassMinion,
        minion_config=MinionConfig(stall=False),
    )
    unselected_start = OrchestrationStart(
        pipeline=BlockedPipeline,
        minion=SameClassMinion,
        minion_config=MinionConfig(stall=True),
    )
    directives: list[Directive] = [
        selected_start,
        unselected_start,
        WaitWorkflowCompletions(orchestrations=(selected_start,)),
        OrchestrationStop(id=selected_start, expect_success=True),
        OrchestrationStop(id=unselected_start, expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        directives,
        pipeline_event_counts={ReadyPipeline: 1, BlockedPipeline: 1},
        per_verification_timeout=0.05,
    )


@pytest.mark.asyncio
async def test_wait_workflow_completions_does_not_use_unselected_same_class_calls(
    gru: Gru,
):
    from tests.assets.minions.two_steps.simple.default import AssetMinion
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as SelectedPipeline,
    )
    from tests.assets.pipelines.emit_one.simple.default_b import (
        AssetPipeline as UnselectedPipeline,
    )
    from tests.support.gru_scenario.plan import ScenarioPlan
    from tests.support.gru_scenario.runner import ScenarioRunner

    SelectedPipeline.configure_gate(expected_subs=2)
    UnselectedPipeline.configure_gate(expected_subs=1)
    selected_start = OrchestrationStart(
        pipeline=SelectedPipeline,
        minion=AssetMinion,
    )
    unselected_start = OrchestrationStart(
        pipeline=UnselectedPipeline,
        minion=AssetMinion,
    )
    plan = ScenarioPlan(
        [
            unselected_start,
            selected_start,
            WaitWorkflowCompletions(orchestrations=(selected_start,)),
        ],
        pipeline_event_counts={SelectedPipeline: 1, UnselectedPipeline: 1},
    )

    try:
        with pytest.raises(TimeoutError):
            await ScenarioRunner(gru, plan, per_verification_timeout=0.05).run()
    finally:
        shutdown = await gru.shutdown()
        assert shutdown.success


@pytest.mark.asyncio
async def test_exact_runtime_expectation_reports_mismatch(
    gru: Gru,
):
    from tests.assets.minions.two_steps.simple.default import (
        AssetMinion as SimpleMinion,
    )
    from tests.assets.minions.two_steps.simple.with_simple_b_resource import (
        AssetMinion as SimpleResourceBMinion,
    )
    from tests.assets.pipelines.emit_one.simple.default import (
        AssetPipeline as EmitOneSimplePipeline,
    )

    pipeline_ref = EmitOneSimplePipeline.__module__
    EmitOneSimplePipeline.configure_gate(expected_subs=2)

    start = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleMinion.__module__,
    )
    start_other = OrchestrationStart(
        pipeline=pipeline_ref,
        minion=SimpleResourceBMinion.__module__,
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
            directives,
            pipeline_event_counts={pipeline_ref: 1},
        )


@pytest.mark.asyncio
async def test_strict_wait_reports_workflow_window_overlap_mismatch(
    gru: Gru,
    tests_dir: Path,
):
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
            directives,
            pipeline_event_counts={pipeline_ref: 1},
        )


@pytest.mark.asyncio
async def test_wait_for_empty_workflow_set_is_noop(
    gru: Gru,
):
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
        directives,
        pipeline_event_counts={pipeline_ref: 1},
    )
