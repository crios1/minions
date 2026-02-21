import pytest

from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore

class TestInvalidComposition:
    # NOTE: should have test for each case where gru
    # returns error given invalid minion, pipeline, resource

    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_minion_file(self, gru_factory, tests_dir):

            minion_modpath = "tests.assets.file_empty"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "must define a `minion` variable or contain at least one subclass of `Minion`" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_file_with_multiple_minions_and_no_explicit_minion(self, gru_factory, tests_dir):
            
            minion_modpath = "tests.assets.file_with_two_minions"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "multiple Minion subclasses but no explicit `minion` variable" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_file_with_invalid_explicit_minion(self, gru_factory, tests_dir):

            minion_modpath = "tests.assets.file_with_invalid_explicit_minion"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "is not a subclass of Minion" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_workflow_context_not_serializable(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.file_with_unserializable_workflow_context_minion"
            pipeline_modpath = "tests.assets.pipeline_single_event"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "workflow context is not JSON-serializable" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_event_not_serializable(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.invalid.bad_event"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "event type is not JSON-serializable" in result.reason

    class TestPipelineFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_pipeline_file(self, gru_factory, tests_dir):

            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.file_empty"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "must define a `pipeline` variable or contain at least one subclass of `Pipeline`" in result.reason

        @pytest.mark.asyncio 
        async def test_gru_returns_error_on_pipeline_file_with_multiple_pipelines_and_no_explicit_pipeline(self, gru_factory, tests_dir):
            
            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.file_with_two_pipelines"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "multiple Pipeline subclasses but no explicit `pipeline` variable" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_pipeline_file_with_invalid_explicit_pipeline(self, gru_factory, tests_dir):

            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.file_with_invalid_explicit_pipeline"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "is not a subclass of Pipeline" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_pipeline_event_not_serializable(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.pipelines.invalid.unserializable_event"
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            async with gru_factory(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            ) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath
                )

                assert not result.success
                assert result.reason
                assert "event type is not JSON-serializable" in result.reason

    # Resource doesn't have tests like in TestMinion and TestPipeline
    # because Resources dependencies are declared as type hints
    # when creating Minion and Pipeline subclasses.

    # TODO: ensure gru properly handles Minions and Pipelines with multiple Resource dependency declarations
class TestInvalidCompositionUsingNewAssets:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_minion_file(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.entrypoints.invalid.empty"
            pipeline_modpath = "tests.assets.pipelines.emit1.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "must define a `minion` variable" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_file_with_multiple_minions_and_no_explicit_minion(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.entrypoints.invalid.two_minions"
            pipeline_modpath = "tests.assets.pipelines.emit1.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "multiple Minion subclasses" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_file_with_invalid_explicit_minion(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.entrypoints.invalid.invalid_explicit_minion"
            pipeline_modpath = "tests.assets.pipelines.emit1.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "is not a subclass of Minion" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_workflow_context_not_serializable(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.minions.invalid.bad_context"
            pipeline_modpath = "tests.assets.pipelines.emit1.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "workflow context is not JSON-serializable" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_event_not_serializable(
            self, gru_factory, tests_dir
        ):
            minion_modpath = "tests.assets.minions.invalid.bad_event"
            pipeline_modpath = "tests.assets.pipelines.emit1.emit_1"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "event type is not JSON-serializable" in result.reason

    class TestPipelineFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_pipeline_file(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.two_steps.basic"
            pipeline_modpath = "tests.assets.entrypoints.invalid.empty"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "must define a `pipeline` variable" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_pipeline_file_with_multiple_pipelines_and_no_explicit_pipeline(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.two_steps.basic"
            pipeline_modpath = "tests.assets.entrypoints.invalid.two_pipelines"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "multiple Pipeline subclasses" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_pipeline_file_with_invalid_explicit_pipeline(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.two_steps.basic"
            pipeline_modpath = "tests.assets.entrypoints.invalid.invalid_explicit_pipeline"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "is not a subclass of Pipeline" in result.reason

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_pipeline_event_not_serializable(self, gru_factory, tests_dir):
            minion_modpath = "tests.assets.minions.two_steps.basic"
            pipeline_modpath = "tests.assets.pipelines.invalid.unserializable_event"
            config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "event type is not JSON-serializable" in result.reason
