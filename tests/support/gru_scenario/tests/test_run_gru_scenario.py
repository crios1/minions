import pytest

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


@pytest.mark.asyncio
async def test_run_gru_scenario_with_new_assets(gru, tests_dir, logger, metrics, state_store
):
    cfg1 = str(tests_dir / "assets" / "config" / "minions" / "a.toml")
    cfg2 = str(tests_dir / "assets" / "config" / "minions" / "b.toml")
    pipeline_modpath = "tests.assets.pipelines.sync.counter.sync_2subs_2events"

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.counter.resourced",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
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
async def test_run_gru_scenario_helper_basic(gru, tests_dir, logger, metrics, state_store):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
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
async def test_run_gru_scenario_batches_stops_serial(gru, tests_dir, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.resourced_2",
            minion_config_path=cfg2,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflows(),
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
async def test_run_gru_scenario_duplicate_start_fails(gru, tests_dir, logger, metrics, state_store):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
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
async def test_run_gru_scenario_failed_start_does_not_require_minion_startup(gru, tests_dir, logger, metrics, state_store):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    minion_modpath = "tests.assets.minions.two_steps.simple.basic"
    pipeline_modpath = "tests.assets.pipelines.simple.dict_event"

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
async def test_run_gru_scenario_stop_unknown_fails(gru, tests_dir, logger, metrics, state_store):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        WaitWorkflows(),
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
async def test_run_gru_scenario_parallel_starts(gru, tests_dir, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
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
async def test_run_gru_scenario_wait_workflows_subset(gru, tests_dir, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.resourced_2",
            minion_config_path=cfg2,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflows(minion_names={"simple-minion"}),
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
async def test_run_gru_scenario_wait_workflows_unknown_name_fails(gru, tests_dir, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=1)

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
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
async def test_run_gru_scenario_wait_workflows_empty_is_noop(gru, tests_dir, logger, metrics, state_store):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=cfg1,
            pipeline=pipeline_modpath,
            expect=MinionRunSpec(),
        ),
        WaitWorkflows(minion_names=set()),
        WaitWorkflows(),
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
async def test_run_gru_scenario_parallel_mixed_directives(gru, tests_dir, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives: list[Directive] = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
        Concurrent(
            WaitWorkflows(minion_names=set()),
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
async def test_run_gru_scenario_minion_call_overrides(gru, tests_dir, logger, metrics, state_store):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives: list[Directive] = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
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
async def test_dsl_exploration(gru, tests_dir, logger, metrics, state_store):
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path=str(tests_dir / "assets" / "config/minions/a.toml"),
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(minion_call_overrides={"step_1": 0}),
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                minion_config_path=str(tests_dir / "assets" / "config/minions/b.toml"),
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
        ),
        WaitWorkflows(),
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
async def test_run_gru_scenario_golden_regression_mixed_concurrent_wait_subset(gru, tests_dir, logger, metrics, state_store, reload_wait_for_subs_pipeline
):
    cfg = str(tests_dir / "assets" / "config/minions/a.toml")
    minion_modpath = "tests.assets.minions.two_steps.simple.basic"
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
