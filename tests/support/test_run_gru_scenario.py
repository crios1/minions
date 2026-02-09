import pytest

from pathlib import Path

from tests.support.gru_scenario import (
    Directive,
    MinionRunSpec,
    GruShutdown,
    MinionStart,
    MinionStop,
    Concurrent,
    WaitWorkflows,
    run_gru_scenario,
)


TESTS_DIR = Path(__file__).resolve().parents[1]

@pytest.mark.asyncio
async def test_run_gru_scenario_with_new_assets(
    gru, logger, metrics, state_store, reload_emit_n_pipeline
):
    cfg1 = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
    cfg2 = str(TESTS_DIR / "assets_new" / "minion_config_b.toml")
    pipeline_modpath = "tests.assets_new.pipeline_emit_n"
    reload_emit_n_pipeline(expected_subs=2, total_events=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets_new.minion_two_steps",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets_new.minion_two_steps_resourced",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
        MinionStop(expect_success=True, name_or_instance_id="two-step-minion"),
        MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-minion"),
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
async def test_run_gru_scenario_helper_basic(gru, logger, metrics, state_store):
    config_path = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        WaitWorkflows(),
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
    gru, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    cfg2 = str(TESTS_DIR / "assets" / "minion_config_simple_2.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        MinionStart(
            minion="tests.assets.minion_simple_resourced_2",
            minion_config_path=cfg2,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflows(),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
        MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-2"),
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
async def test_run_gru_scenario_duplicate_start_fails(gru, logger, metrics, state_store):
    config_path = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect_success=False,
        ),
        WaitWorkflows(),
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
async def test_run_gru_scenario_failed_start_does_not_require_minion_startup(gru, logger, metrics, state_store):
    config_path = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    minion_modpath = "tests.assets.minion_simple"
    pipeline_modpath = "tests.assets.pipeline_dict"

    directives: list[Directive] = [
        MinionStart(
            minion=minion_modpath,
            minion_config_path=config_path,
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
async def test_run_gru_scenario_stop_unknown_fails(gru, logger, metrics, state_store):
    config_path = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        WaitWorkflows(),
        MinionStop(expect_success=False, name_or_instance_id="missing-minion"),
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
    gru, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    cfg2 = str(TESTS_DIR / "assets" / "minion_config_simple_2.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minion_simple",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_2",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
        MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-2"),
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
    gru, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    cfg2 = str(TESTS_DIR / "assets" / "minion_config_simple_2.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        MinionStart(
            minion="tests.assets.minion_simple_resourced_2",
            minion_config_path=cfg2,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflows(minion_names={"simple-minion"}),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
        MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-2"),
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
    gru, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=1)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        WaitWorkflows(minion_names={"missing-minion"}),
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
async def test_run_gru_scenario_wait_workflows_empty_is_noop(gru, logger, metrics, state_store):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        WaitWorkflows(minion_names=set()),
        WaitWorkflows(),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
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
    gru, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    cfg2 = str(TESTS_DIR / "assets" / "minion_config_simple_2.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minion_simple",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_2",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
        Concurrent(
            WaitWorkflows(minion_names=set()),
            MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-2"),
        ),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
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
async def test_run_gru_scenario_minion_call_overrides(gru, logger, metrics, state_store):
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(minion_call_overrides={"step_1": 0}),
        ),
        WaitWorkflows(),
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
async def test_dsl_exploration(gru, logger, metrics, state_store):
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minion_simple",
                minion_config_path=str(TESTS_DIR / "assets" / "minion_config_simple_1.toml"),
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(minion_call_overrides={"step_1": 0}),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_2",
                minion_config_path=str(TESTS_DIR / "assets" / "minion_config_simple_2.toml"),
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
        ),
        WaitWorkflows(),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
        MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-2"),
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
    gru, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    minion_modpath = "tests.assets.minion_simple"
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=1)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion=minion_modpath,
                minion_config_path=cfg,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion=minion_modpath,
                minion_config_path=cfg,
                pipeline=pipeline_modpath,
                expect_success=False,
            ),
        ),
        WaitWorkflows(minion_names={"simple-minion"}),
        MinionStop(expect_success=True, name_or_instance_id="simple-minion"),
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
