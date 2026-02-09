import asyncio
import pytest

from minions._internal._domain.gru import Gru
from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.support.gru_scenario import (
    GruShutdown,
    MinionRunSpec,
    MinionStart,
    MinionStop,
    WaitWorkflows,
    run_gru_scenario,
)

class TestInvalidUsage:
    @pytest.mark.asyncio
    async def test_gru_raises_on_direct_instantiation(self):
        with pytest.raises(RuntimeError):
            Gru(
                loop=asyncio.get_running_loop(),
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics()
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_multiple_instances(self, gru_factory):
        async with gru_factory(
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            state_store=NoOpStateStore()
        ):
            with pytest.raises(RuntimeError, match="Only one Gru instance is allowed per process."):
                await Gru.create(
                    logger=NoOpLogger(),
                    metrics=NoOpMetrics(),
                    state_store=NoOpStateStore()
                )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_logger", [123, "invalid"])
    async def test_gru_raises_on_invalid_logger_param(self, bad_logger):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=bad_logger,
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics", [123, "invalid"])
    async def test_gru_raises_on_invalid_metrics_param(self, bad_metrics):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=bad_metrics,
                state_store=NoOpStateStore()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_state_store", [123, "invalid"])
    async def test_gru_raises_on_invalid_state_store_param(self, bad_state_store):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=bad_state_store
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_minion(self, gru_factory, tests_dir):
        # TODO:
        # - start 2 minions with the same name (would need to start different minion but give the same name)
        # - stop minion by name => and get error as a value
        # TODO: but actually runs with events be created and stuff?

        print('--------- start problematic test ---------')

        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result1 = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath
            )

            print(result1)

            assert result1.success
            assert result1.name == "simple-minion"
            assert result1.instance_id in gru._minions_by_id
            assert result1.instance_id in gru._minion_tasks

            result2 = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath
            )

            print(result2)

            assert not result2.success
            assert result2.reason
            assert "Minion already running" in result2.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_minion(self, gru_factory):
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.stop_minion('mock') 

            print(result)

            assert not result.success
            assert result.reason
            assert "No minion found with the given name or instance ID" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(self, gru_factory, tests_dir):
        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_dict"
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

            print(result)

            assert not result.success
            assert result.reason
            assert "Incompatible minion and pipeline event types" in result.reason


    # would need to be run in gru ...
    # def test_invalid_user_code_in_step(self):
    #     with pytest.raises(Exception):
    #         class MyMinion(Minion[dict,dict]):
    #             @minion_step
    #             async def step_1(self):
    #                 import asyncio
    #                 async def _(): ...
    #                 asyncio.create_task(_())
                    


class TestInvalidUsageDSL:
    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_minion(
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
    async def test_gru_returns_error_when_stopping_nonexistant_minion(
        self, gru, logger, metrics, state_store
    ):
        directives = [
            MinionStop(expect_success=False, name_or_instance_id="mock"),
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
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(
        self, gru, logger, metrics, state_store, tests_dir
    ):
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        directives = [
            MinionStart(
                minion="tests.assets.minion_simple",
                minion_config_path=config_path,
                pipeline="tests.assets.pipeline_dict",
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
class TestInvalidUsageUsingNewAssets:
    @pytest.mark.asyncio
    async def test_gru_raises_on_direct_instantiation(self):
        with pytest.raises(RuntimeError):
            Gru(
                loop=asyncio.get_running_loop(),
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics(),
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_multiple_instances(self, gru_factory):
        async with gru_factory(
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            state_store=NoOpStateStore(),
        ):
            with pytest.raises(RuntimeError, match="Only one Gru instance is allowed per process."):
                await Gru.create(
                    logger=NoOpLogger(),
                    metrics=NoOpMetrics(),
                    state_store=NoOpStateStore(),
                )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_logger", [123, "invalid"])
    async def test_gru_raises_on_invalid_logger_param(self, bad_logger):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=bad_logger,
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics", [123, "invalid"])
    async def test_gru_raises_on_invalid_metrics_param(self, bad_metrics):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=bad_metrics,
                state_store=NoOpStateStore(),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_state_store", [123, "invalid"])
    async def test_gru_raises_on_invalid_state_store_param(self, bad_state_store):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=bad_state_store,
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_minion(
        self, gru_factory, reload_emit_n_pipeline, tests_dir
    ):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        config_path = str(tests_dir / "assets_new" / "minion_config_a.toml")
        reload_emit_n_pipeline(expected_subs=1, total_events=1)

        async with gru_factory(state_store=NoOpStateStore(), logger=ConsoleLogger(), metrics=NoOpMetrics()) as gru:
            result1 = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
            )

            assert result1.success
            assert result1.name == "two-step-minion"
            assert result1.instance_id in gru._minions_by_id
            assert result1.instance_id in gru._minion_tasks

            result2 = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
            )

            assert not result2.success
            assert result2.reason
            assert "Minion already running" in result2.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_minion(self, gru_factory):
        async with gru_factory(state_store=NoOpStateStore(), logger=ConsoleLogger(), metrics=NoOpMetrics()) as gru:
            result = await gru.stop_minion("mock")

            assert not result.success
            assert result.reason
            assert "No minion found" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(self, gru_factory, tests_dir):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_dict_event"
        config_path = str(tests_dir / "assets_new" / "minion_config_a.toml")

        async with gru_factory(state_store=NoOpStateStore(), logger=ConsoleLogger(), metrics=NoOpMetrics()) as gru:
            result = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
            )

            assert not result.success
            assert result.reason
            assert "Incompatible minion and pipeline event types" in result.reason
