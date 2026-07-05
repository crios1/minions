import contextlib
from collections.abc import Callable

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_runtime_component_counts_exact,
)


class TestMinionFile:
    @pytest.mark.asyncio
    async def test_gru_accepts_file_with_multiple_minions_and_explicit_minion(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = "tests.assets.entrypoints.valid.two_minions_explicit_minion"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                pipeline=pipeline_module_path,
            )

            assert result.success

    @pytest.mark.asyncio
    async def test_gru_accepts_file_with_single_minion_subclass(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = "tests.assets.entrypoints.valid.single_minion_subclass"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                pipeline=pipeline_module_path,
            )

            assert result.success

    @pytest.mark.asyncio
    async def test_gru_starts_minion_with_multiple_distinct_resource_dependencies(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = (
            "tests.assets.minions.one_step.counter."
            "with_fixed_and_fixed_b_resources"
        )
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                pipeline=pipeline_module_path,
            )

            assert result.success
            assert_runtime_component_counts_exact(gru, pipelines=1, resources=2)
            assert result.orchestration_id is not None


class TestPipelineFile:
    @pytest.mark.asyncio
    async def test_gru_accepts_file_with_single_pipeline_class(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.pipelines.entrypoint.counter.single_class"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                pipeline=pipeline_module_path,
            )

            assert result.success

    @pytest.mark.asyncio
    async def test_gru_accepts_file_with_single_pipeline_subclass(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.entrypoints.valid.single_pipeline_subclass"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                pipeline=pipeline_module_path,
            )

            assert result.success
