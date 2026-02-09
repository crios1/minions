import pytest

from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.support.gru_scenario import (
    Directive,
    Concurrent,
    GruShutdown,
    MinionRunSpec,
    MinionStart,
    MinionStop,
    WaitWorkflows,
    run_gru_scenario,
)


class TestValidUsage:
    @pytest.mark.asyncio
    async def test_gru_accepts_none_logger_metrics_state_store(self, gru_factory):
        async with gru_factory(
            logger=None,
            state_store=None,
            metrics=None
        ):
            pass

    @pytest.mark.asyncio
    async def test_gru_allows_create_and_immediate_shutdown(self, gru_factory):
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ):
            pass

    # TODO: test that pipeline event processed by minion for methods below

    @pytest.mark.asyncio
    async def test_gru_start_stop_minion(self, gru_factory, reload_wait_for_subs_pipeline, tests_dir):
        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
        reload_wait_for_subs_pipeline(expected_subs=3)
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath
            )

            assert result.success
            assert result.name == "simple-minion"
            assert result.instance_id in gru._minions_by_id
            assert result.instance_id in gru._minion_tasks

            await gru.stop_minion(result.instance_id)

    @pytest.mark.asyncio
    async def test_gru_start_minion_shutdown_without_stop(self, gru_factory, tests_dir):
        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath
            )

            assert result.success
            assert result.name == "simple-minion"
            assert result.instance_id in gru._minions_by_id
            assert result.instance_id in gru._minion_tasks

    # TODO: check fanouts and such for methods below

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(self, gru_factory, tests_dir):
        """
        Start three minions each with their own pipeline and their own Resource type
        so there is no sharing of pipelines or resources between minions.
        """
        minion1 = "tests.assets.minion_simple_resourced_1"
        minion2 = "tests.assets.minion_simple_resourced_2"
        minion3 = "tests.assets.minion_simple_resourced_3"

        pipeline1 = "tests.assets.pipeline_simple_single_event_1"
        pipeline2 = "tests.assets.pipeline_simple_single_event_2"
        pipeline3 = "tests.assets.pipeline_simple_single_event_3"
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        cfg2 = str(tests_dir / "assets" / "minion_config_simple_2.toml")
        cfg3 = str(tests_dir / "assets" / "minion_config_simple_3.toml")

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        ) as gru:
            r1 = await gru.start_minion(minion=minion1, minion_config_path=cfg1, pipeline=pipeline1)
            r2 = await gru.start_minion(minion=minion2, minion_config_path=cfg2, pipeline=pipeline2)
            r3 = await gru.start_minion(minion=minion3, minion_config_path=cfg3, pipeline=pipeline3)

            assert r1.success and r2.success and r3.success

            # Expect three distinct pipeline IDs
            assert len(gru._pipelines) >= 3

            # Expect three distinct resource classes started
            assert len(gru._resources) >= 3

            # stop them
            assert r1.instance_id is not None
            await gru.stop_minion(r1.instance_id)
            assert r2.instance_id is not None
            await gru.stop_minion(r2.instance_id)
            assert r3.instance_id is not None
            await gru.stop_minion(r3.instance_id)

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(
        self, gru_factory, reload_wait_for_subs_pipeline, tests_dir
    ):
        """
        Start three minions that share the same pipeline and a single Resource type.
        Verify pipeline and resource are shared and cleaned up after stopping all minions.
        """
        minion_modpath = "tests.assets.minion_simple_resourced_1"
        pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
        reload_wait_for_subs_pipeline(expected_subs=3)
        from tests.assets.minion_simple_resourced_1 import SimpleResourcedMinion1
        SimpleResourcedMinion1.enable_spy()
        SimpleResourcedMinion1.reset_spy()

        # TODO: i'm testing resource sharing between minions spawned from same minion class but different configs
        # i should also test the case where i spawn from seperate minion classes/files
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        cfg2 = str(tests_dir / "assets" / "minion_config_simple_2.toml")
        cfg3 = str(tests_dir / "assets" / "minion_config_simple_3.toml")

        # TODO: consider refactoring gru to have the kwargs be classes instead of instances
        # it might be cleaner and then the user wont have to manually wire things like this
        # and cuz then gru can handle instantiation and startup
        # but what if the use want to bring-thier-own and wants to instantiate with parameters?
        # ask copilot
        # !! will have to do the update across this whole test file !!

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        ) as gru:
            r1 = await gru.start_minion(minion=minion_modpath, minion_config_path=cfg1, pipeline=pipeline_modpath)
            r2 = await gru.start_minion(minion=minion_modpath, minion_config_path=cfg2, pipeline=pipeline_modpath)
            r3 = await gru.start_minion(minion=minion_modpath, minion_config_path=cfg3, pipeline=pipeline_modpath)

            assert r1.success and r2.success and r3.success

            # pipeline should be shared (single id)
            assert len(gru._pipelines) == 1

            # resource should be shared across minions
            assert len(gru._resources) == 1

            await SimpleResourcedMinion1.wait_for_calls(
                expected={"step_1": 3, "step_2": 3},
                timeout=5.0,
            )

            # stop minions and assert cleanup
            assert r1.instance_id is not None
            await gru.stop_minion(r1.instance_id)
            assert len(gru._pipelines) == 1
            assert r2.instance_id is not None
            await gru.stop_minion(r2.instance_id)
            assert len(gru._pipelines) == 1
            assert r3.instance_id is not None
            await gru.stop_minion(r3.instance_id)

            # after all stopped, pipeline and resources cleaned
            assert len(gru._pipelines) == 0
            assert len(gru._resources) == 0

    @pytest.mark.asyncio
    async def test_minion_and_pipeline_share_resource_dependency(self, gru_factory, tests_dir):
        minion_modpath = "tests.assets.minion_simple_resourced_1"
        pipeline_modpath = "tests.assets.pipeline_simple_resourced"
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        ) as gru:
            r1 = await gru.start_minion(minion=minion_modpath, minion_config_path=cfg1, pipeline=pipeline_modpath)

            assert r1.success

            assert len(gru._pipelines) == 1
            assert len(gru._resources) == 1

            assert isinstance(r1.instance_id, str)
            await gru.stop_minion(r1.instance_id)

            assert len(gru._pipelines) == 0
            assert len(gru._resources) == 0

    # TODO: I need tests for gru's default usages to ensure i stay version 1.x.x compliant



class TestValidUsageDSL:
    @pytest.mark.asyncio
    async def test_gru_start_stop_minion(
        self, gru, logger, metrics, state_store, reload_wait_for_subs_pipeline, tests_dir
    ):
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
        reload_wait_for_subs_pipeline(expected_subs=1)

        directives = [
            MinionStart(
                minion="tests.assets.minion_simple",
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
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
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(
        self, gru, logger, metrics, state_store, tests_dir
    ):
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        cfg2 = str(tests_dir / "assets" / "minion_config_simple_2.toml")
        cfg3 = str(tests_dir / "assets" / "minion_config_simple_3.toml")

        pipeline1 = "tests.assets.pipeline_simple_single_event_1"
        pipeline2 = "tests.assets.pipeline_simple_single_event_2"
        pipeline3 = "tests.assets.pipeline_simple_single_event_3"

        directives = [
            MinionStart(
                minion="tests.assets.minion_simple_resourced_1",
                minion_config_path=cfg1,
                pipeline=pipeline1,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_2",
                minion_config_path=cfg2,
                pipeline=pipeline2,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_3",
                minion_config_path=cfg3,
                pipeline=pipeline3,
                expect=MinionRunSpec(),
            ),
            WaitWorkflows(),
            MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-1"),
            MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-2"),
            MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-3"),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={
                pipeline1: 1,
                pipeline2: 1,
                pipeline3: 1,
            },
        )

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(
        self, gru, logger, metrics, state_store, reload_wait_for_subs_pipeline, tests_dir
    ):
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        cfg2 = str(tests_dir / "assets" / "minion_config_simple_2.toml")
        cfg3 = str(tests_dir / "assets" / "minion_config_simple_3.toml")
        pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
        reload_wait_for_subs_pipeline(expected_subs=3)

        directives = [
            MinionStart(
                minion="tests.assets.minion_simple_resourced_1",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_1",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_1",
                minion_config_path=cfg3,
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
    async def test_minion_and_pipeline_share_resource_dependency(
        self, gru, logger, metrics, state_store, tests_dir
    ):
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        pipeline_modpath = "tests.assets.pipeline_simple_resourced"

        directives = [
            MinionStart(
                minion="tests.assets.minion_simple_resourced_1",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            WaitWorkflows(),
            MinionStop(expect_success=True, name_or_instance_id="simple-resourced-minion-1"),
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
    async def test_gru_start_minion_shutdown_without_stop(
        self, gru, logger, metrics, state_store, tests_dir
    ):
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

        directives = [
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

class TestValidUsageUsingNewAssetsDSL:
    @pytest.mark.asyncio
    async def test_gru_accepts_none_logger_metrics_state_store(self, gru_factory):
        async with gru_factory(logger=None, state_store=None, metrics=None):
            pass

    @pytest.mark.asyncio
    async def test_gru_allows_create_and_immediate_shutdown(self, gru_factory):
        async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()):
            pass

    @pytest.mark.asyncio
    async def test_gru_start_stop_minion(
        self, gru, logger, metrics, state_store, reload_emit_n_pipeline, tests_dir
    ):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        config_path = str(tests_dir / "assets_new" / "minion_config_a.toml")
        reload_emit_n_pipeline(expected_subs=1, total_events=1)

        directives: list[Directive] = [
            MinionStart(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            WaitWorkflows(),
            MinionStop(expect_success=True, name_or_instance_id="two-step-minion"),
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
    async def test_gru_start_minion_shutdown_without_stop(
        self, gru, logger, metrics, state_store, reload_emit_n_pipeline, tests_dir
    ):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        config_path = str(tests_dir / "assets_new" / "minion_config_a.toml")
        reload_emit_n_pipeline(expected_subs=1, total_events=1)

        directives: list[Directive] = [
            MinionStart(
                minion=minion_modpath,
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
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(
        self, gru, logger, metrics, state_store, reload_emit_n_variant, tests_dir
    ):
        minion1 = "tests.assets_new.minion_two_steps_resourced"
        minion2 = "tests.assets_new.minion_two_steps_resourced_b"
        minion3 = "tests.assets_new.minion_two_steps_resourced_c"

        pipeline1 = "tests.assets_new.pipeline_emit_n_a"
        pipeline2 = "tests.assets_new.pipeline_emit_n_b"
        pipeline3 = "tests.assets_new.pipeline_emit_n_c"

        cfg1 = str(tests_dir / "assets_new" / "minion_config_a.toml")
        cfg2 = str(tests_dir / "assets_new" / "minion_config_b.toml")
        cfg3 = str(tests_dir / "assets_new" / "minion_config_c.toml")

        reload_emit_n_variant(pipeline1, expected_subs=1, total_events=1)
        reload_emit_n_variant(pipeline2, expected_subs=1, total_events=1)
        reload_emit_n_variant(pipeline3, expected_subs=1, total_events=1)

        directives: list[Directive] = [
            Concurrent(
                MinionStart(minion=minion1, minion_config_path=cfg1, pipeline=pipeline1),
                MinionStart(minion=minion2, minion_config_path=cfg2, pipeline=pipeline2),
                MinionStart(minion=minion3, minion_config_path=cfg3, pipeline=pipeline3),
            ),
            WaitWorkflows(),
            MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-minion"),
            MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-minion-b"),
            MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-minion-c"),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline1: 1, pipeline2: 1, pipeline3: 1},
            )

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(
        self, gru, logger, metrics, state_store, reload_emit_n_pipeline, tests_dir
    ):
        minion_a = "tests.assets_new.minion_two_steps_resourced"
        minion_b = "tests.assets_new.minion_two_steps_resourced_shared_b"
        minion_c = "tests.assets_new.minion_two_steps_resourced_shared_c"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        reload_emit_n_pipeline(expected_subs=3, total_events=1)

        cfg1 = str(tests_dir / "assets_new" / "minion_config_a.toml")
        cfg2 = str(tests_dir / "assets_new" / "minion_config_b.toml")
        cfg3 = str(tests_dir / "assets_new" / "minion_config_c.toml")

        directives: list[Directive] = [
            MinionStart(minion=minion_a, minion_config_path=cfg1, pipeline=pipeline_modpath),
            MinionStart(minion=minion_b, minion_config_path=cfg2, pipeline=pipeline_modpath),
            MinionStart(minion=minion_c, minion_config_path=cfg3, pipeline=pipeline_modpath),
            WaitWorkflows(),
            MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-minion"),
            MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-shared-minion-b"),
            MinionStop(expect_success=True, name_or_instance_id="two-step-resourced-shared-minion-c"),
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
    async def test_minion_and_pipeline_share_resource_dependency(
        self, gru, logger, metrics, state_store, reload_pipeline_module, tests_dir
    ):
        minion_modpath = "tests.assets_new.minion_two_steps_resourced"
        pipeline_modpath = "tests.assets_new.pipeline_resourced"
        config_path = str(tests_dir / "assets_new" / "minion_config_a.toml")
        reload_pipeline_module(pipeline_modpath)

        directives: list[Directive] = [
            MinionStart(minion=minion_modpath, minion_config_path=config_path, pipeline=pipeline_modpath),
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
