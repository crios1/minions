import pytest

from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

class TestValidComposition:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_multiple_minions_and_explicit_minion(self, gru_factory, tests_dir):
            
            minion_modpath = "tests.assets.entrypoints.valid.two_minions_explicit_minion_simple"
            pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"
            config_path = str(tests_dir / "assets" / "config/minions/a.toml")

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

        @pytest.mark.asyncio
        async def test_gru_starts_minion_with_multiple_distinct_resource_dependencies(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.two_steps.simple.resourced_multi"
            pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"
            config_path = str(tests_dir / "assets" / "config/minions/a.toml")

            logger = InMemoryLogger()
            async with gru_factory(
                state_store=InMemoryStateStore(logger=logger),
                logger=logger,
                metrics=InMemoryMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert result.success

                assert len(gru._pipelines) >= 1
                assert len(gru._resources) >= 2

                assert result.instance_id is not None

    class TestPipelineFile: # TODO: consider implementing tests to be in parity with TestMinionFile class
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_single_pipeline_class(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.two_steps.simple.basic"
            pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"
            config_path = str(tests_dir / "assets" / "config/minions/a.toml")

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


class TestValidCompositionUsingNewAssets:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_multiple_minions_and_explicit_minion(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.entrypoints.valid.two_minions_explicit_minion"
            pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=ConsoleLogger(),
                metrics=NoOpMetrics(),
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )
                assert result.success

        @pytest.mark.asyncio
        async def test_gru_starts_minion_with_multiple_distinct_resource_dependencies(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.minions.two_steps.counter.multi_resources"
            pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            logger = InMemoryLogger()
            async with gru_factory(
                state_store=InMemoryStateStore(logger=logger),
                logger=logger,
                metrics=InMemoryMetrics(),
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert result.success
                assert len(gru._pipelines) >= 1
                assert len(gru._resources) >= 2
                assert result.instance_id is not None

    class TestPipelineFile:
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_single_pipeline_class(
            self, gru_factory, reload_pipeline_module, tests_dir
        ):
            minion_modpath = "tests.assets.minions.two_steps.counter.basic"
            pipeline_modpath = "tests.assets.pipelines.entrypoint.counter.single_class"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")
            reload_pipeline_module(pipeline_modpath)

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=ConsoleLogger(),
                metrics=NoOpMetrics(),
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )
                assert result.success
