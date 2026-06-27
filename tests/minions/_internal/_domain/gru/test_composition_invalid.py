import contextlib
from collections.abc import Callable
from pathlib import Path

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore


class TestMinionFile:
    @pytest.mark.asyncio
    async def test_gru_returns_error_on_empty_minion_file(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.entrypoints.invalid.empty"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "must define a `minion` variable" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_minion_file_with_multiple_minions_and_no_explicit_minion(  # noqa: E501
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.entrypoints.invalid.two_minions"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "multiple Minion subclasses" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_minion_file_with_invalid_explicit_minion(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.entrypoints.invalid.invalid_explicit_minion"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "is not a subclass of Minion" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_minion_unsupported_workflow_context_type(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.invalid.unsupported_context_type"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert (
                "workflow context type is not supported. Supported user-declared types"
                in result.reason
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_minion_workflow_context_not_serializable(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.invalid.unserializable_context"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "workflow context type is not serializable" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_minion_unsupported_event_type(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.invalid.unsupported_event_type"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "event type is not supported. Supported user-declared types" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_minion_event_not_serializable(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.invalid.unserializable_event"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "event type is not serializable" in result.reason


class TestPipelineFile:
    @pytest.mark.asyncio
    async def test_gru_returns_error_on_empty_pipeline_file(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.entrypoints.invalid.empty"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "must define a `pipeline` variable" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_pipeline_file_with_multiple_pipelines_and_no_explicit_pipeline(  # noqa: E501
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.entrypoints.invalid.two_pipelines"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "multiple Pipeline subclasses" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_pipeline_file_with_invalid_explicit_pipeline(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.entrypoints.invalid.invalid_explicit_pipeline"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "is not a subclass of Pipeline" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_pipeline_unsupported_event_type(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.pipelines.invalid.unsupported_event_type"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "event type is not supported. Supported user-declared types" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_on_pipeline_event_not_serializable(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.pipelines.invalid.with_unserializable_event"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert not result.success
            assert result.reason
            assert "event type is not serializable" in result.reason


# Resources do not have module-entrypoint tests equivalent to Minions and Pipelines:
# Resource dependencies are declared as type hints on component subclasses.
