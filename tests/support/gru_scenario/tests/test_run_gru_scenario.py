from collections.abc import Callable
from pathlib import Path

import pytest

from minions._internal._domain.gru import Gru

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.support.gru_scenario import (
    Directive,
    ExpectRuntime,
    GruShutdown,
    MinionStart,
    MinionStop,
    Concurrent,
    RuntimeExpectSpec,
    AfterWorkflowStarts,
    WaitWorkflowCompletions,
    run_gru_scenario,
)


@pytest.mark.asyncio
async def test_run_gru_scenario_with_new_assets(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_modpath = "tests.assets.pipelines.sync.counter.sync_2subs_2events"

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.counter.resourced",
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="two-step-minion", expect_success=True),
        MinionStop(name_or_instance_id="two-step-resourced-minion", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 2},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_helper_basic(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
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
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflow_starts_then_stop_happy_path(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.failure.abort_step"
    pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        AfterWorkflowStarts(
            expected={"abort-step-minion": 1},
            directive=MinionStop(name_or_instance_id="abort-step-minion", expect_success=True),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_persistence_after_stop(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.failure.slow_step"
    pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        AfterWorkflowStarts(
            expected={"slow-step-minion": 1},
            directive=MinionStop(name_or_instance_id="slow-step-minion", expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(persistence={"slow-step-minion": 1}),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_resolutions_after_completion(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.two_steps.simple.basic"
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={
                    "simple-minion": {"succeeded": 1, "failed": 0, "aborted": 0},
                }
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
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_workflow_steps_exact_after_completion(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.two_steps.simple.basic"
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                workflow_steps={"simple-minion": {"step_1": 1, "step_2": 1}},
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
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_mixed_wait_workflow_step_modes_end_to_end(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath_a = "tests.assets.minions.two_steps.simple.basic"
    minion_modpath_b = "tests.assets.minions.two_steps.simple.resourced_2"
    pipeline_modpath_a = "tests.assets.pipelines.simple.simple_event.single_event_1"
    pipeline_modpath_b = "tests.assets.pipelines.simple.simple_event.single_event_2"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath_a,
            pipeline=pipeline_modpath_a,
        ),
        # Intentional tolerance window for mixed-mode end-to-end coverage.
        WaitWorkflowCompletions(workflow_steps_mode="at_least"),
        MinionStart(
            minion=minion_modpath_b,
            pipeline=pipeline_modpath_b,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={
                    "simple-minion": {"succeeded": 1, "failed": 0, "aborted": 0},
                    "simple-resourced-minion-2": {"succeeded": 1, "failed": 0, "aborted": 0},
                },
                workflow_steps={
                    "simple-minion": {"step_1": 1, "step_2": 1},
                    "simple-resourced-minion-2": {"step_1": 1, "step_2": 1},
                },
                workflow_steps_mode="exact",
            ),
        ),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        MinionStop(name_or_instance_id="simple-resourced-minion-2", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={
            pipeline_modpath_a: 1,
            pipeline_modpath_b: 1,
        },
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_at_checkpoint_index(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.failure.slow_step"
    pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        AfterWorkflowStarts(
            expected={"slow-step-minion": 1},
            directive=MinionStop(name_or_instance_id="slow-step-minion", expect_success=True),
        ),
        ExpectRuntime(
            at=0,
            expect=RuntimeExpectSpec(persistence={"slow-step-minion": 1}),
        ),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_restart_same_pipeline_with_persistence_and_resolutions(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.failure.slow_step"
    pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        AfterWorkflowStarts(
            expected={"slow-step-minion": 1},
            directive=MinionStop(name_or_instance_id="slow-step-minion", expect_success=True),
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                persistence={"slow-step-minion": 1},
                workflow_steps={"slow-step-minion": {"step_1": 1}},
                workflow_steps_mode="exact",
            ),
        ),
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={"slow-step-minion": {"succeeded": 2, "failed": 0, "aborted": 0}},
                workflow_steps={"slow-step-minion": {"step_1": 2}},
                workflow_steps_mode="exact",
            ),
        ),
        MinionStop(name_or_instance_id="slow-step-minion", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_batches_stops_serial(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.resourced_2",
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        MinionStop(name_or_instance_id="simple-resourced-minion-2", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_duplicate_start_fails(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
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
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_failed_start_does_not_require_minion_startup(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    minion_modpath = "tests.assets.minions.two_steps.simple.basic"
    pipeline_modpath = "tests.assets.pipelines.simple.record_event"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            pipeline=pipeline_modpath,
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
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="missing-minion", expect_success=False),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_parallel_starts(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        MinionStop(name_or_instance_id="simple-resourced-minion-2", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflows_subset(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.resourced_2",
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(minion_names={"simple-minion"}, workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        MinionStop(name_or_instance_id="simple-resourced-minion-2", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflows_unknown_name_fails(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=1)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
        ),
        # Intentional unknown-name failure path; mode is irrelevant because wait resolves names first.
        WaitWorkflowCompletions(minion_names={"missing-minion"}),
        GruShutdown(expect_success=True),
    ]

    with pytest.raises(pytest.fail.Exception):
        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_modpath: 1},
        )

@pytest.mark.asyncio
async def test_run_gru_scenario_expect_runtime_exact_reports_mismatch(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
                MinionStart(
                    minion="tests.assets.minions.two_steps.simple.basic",
                    pipeline=pipeline_modpath,
                ),
                MinionStart(
                    minion="tests.assets.minions.two_steps.simple.resourced_2",
                    pipeline=pipeline_modpath,
                ),
            ),
            WaitWorkflowCompletions(workflow_steps_mode="exact"),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    workflow_steps={"simple-minion": {"step_1": 0, "step_2": 0}},
                    workflow_steps_mode="exact",
                ),
            ),
            GruShutdown(expect_success=True),
        ]

    with pytest.raises(
        pytest.fail.Exception,
        match=r"ExpectRuntime\.workflow_steps mismatch for simple-minion\.step_1: expected 0, got 1",
    ):
        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_modpath: 1},
        )


@pytest.mark.asyncio
async def test_run_gru_scenario_strict_wait_workflow_window_overlap_mismatch(
    gru: Gru,
    tests_dir: Path,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_pipeline_module: Callable[[str], None],
) -> None:
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_modpath = "tests.assets.support.pipeline_overlap_window"
    reload_pipeline_module(pipeline_modpath)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.configured",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.configured",
            minion_config_path=cfg2,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        GruShutdown(expect_success=True),
    ]

    # This is the e2e lock for strict-window mismatch diagnostics:
    # workflow-id mismatch must surface first, and include bounded call-count context.
    with pytest.raises(
        pytest.fail.Exception,
        match=r"Checkpoint workflow-id progression mismatch.*expected workflow-id delta 1, got 2\..*Call-count delta: 2 \(expected 1\.\.2\)",
    ):
        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_modpath: 1},
        )


@pytest.mark.asyncio
async def test_run_gru_scenario_wait_workflows_empty_is_noop(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
        ),
        # Intentional no-op path for empty subset handling.
        WaitWorkflowCompletions(minion_names=set()),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_parallel_mixed_directives(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        Concurrent(
            WaitWorkflowCompletions(minion_names=set()),
            MinionStop(name_or_instance_id="simple-resourced-minion-2", expect_success=True),
        ),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )

@pytest.mark.asyncio
async def test_run_gru_scenario_simple_start_wait_shutdown(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> None:
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            pipeline=pipeline_modpath,
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
        pipeline_event_counts={pipeline_modpath: 1},
    )



@pytest.mark.asyncio
async def test_dsl_exploration(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflowCompletions(workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        MinionStop(name_or_instance_id="simple-resourced-minion-2", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )


@pytest.mark.asyncio
async def test_run_gru_scenario_golden_regression_mixed_concurrent_wait_subset(
    gru: Gru,
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    reload_wait_for_subs_pipeline: Callable[..., None],
) -> None:
    minion_modpath = "tests.assets.minions.two_steps.simple.basic"
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=1)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion=minion_modpath,
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion=minion_modpath,
                pipeline=pipeline_modpath,
                expect_success=False,
            ),
        ),
        WaitWorkflowCompletions(minion_names={"simple-minion"}, workflow_steps_mode="exact"),
        MinionStop(name_or_instance_id="simple-minion", expect_success=True),
        GruShutdown(expect_success=True),
    ]

    await run_gru_scenario(
        gru,
        logger,
        metrics,
        state_store,
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )
