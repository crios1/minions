import asyncio
import contextlib
from collections.abc import Callable
from typing import Any, cast

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import assert_orchestration_running
from tests.support.gru_scenario import (
    Directive,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    WaitWorkflowCompletions,
    run_gru_scenario,
)


class TestInvalidUsage:
    @pytest.mark.asyncio
    async def test_gru_raises_on_direct_instantiation(self) -> None:
        with pytest.raises(RuntimeError):
            Gru(
                loop=asyncio.get_running_loop(),
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics(),
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_multiple_instances(self) -> None:
        gru = await Gru.create(
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            state_store=NoOpStateStore()
        )
        try:
            with pytest.raises(RuntimeError, match="Only one Gru instance is allowed per process."):
                await Gru.create(
                    logger=NoOpLogger(),
                    metrics=NoOpMetrics(),
                    state_store=NoOpStateStore()
                )
        finally:
            await gru.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_logger", [123, "invalid"])
    async def test_gru_raises_on_invalid_logger_param(self, bad_logger: object) -> None:
        with pytest.raises(TypeError):
            await Gru.create(
                logger=cast(Any, bad_logger),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics", [123, "invalid"])
    async def test_gru_raises_on_invalid_metrics_param(self, bad_metrics: object) -> None:
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=cast(Any, bad_metrics),
                state_store=NoOpStateStore()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_state_store", [123, "invalid"])
    async def test_gru_raises_on_invalid_state_store_param(self, bad_state_store: object) -> None:
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=cast(Any, bad_state_store)
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_invalid_workflow_persistence_failure_policy(self) -> None:
        with pytest.raises(
            ValueError,
            match=(
                "workflow_persistence_failure_policy must be "
                "'continue-on-failure' or 'idle-until-persisted'"
            ),
        ):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
                workflow_persistence_failure_policy=cast(Any, "invalid"),
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_invalid_workflow_failure_policy(self) -> None:
        with pytest.raises(
            ValueError,
            match="workflow_failure_policy must be 'delete'",
        ):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
                workflow_failure_policy=cast(Any, "invalid"),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwarg", "value", "match"),
        [
            ("workflow_persistence_retry_delay_seconds", 0, "must be a positive number of seconds"),  # noqa: E501
            ("workflow_persistence_retry_max_delay_seconds", 0, "must be a positive number of seconds"),  # noqa: E501
            ("workflow_persistence_retry_backoff_multiplier", 0.5, "must be a number greater than or equal to 1"),  # noqa: E501
            ("workflow_persistence_retry_jitter_ratio", -0.1, "must be a number between 0 and 1"),  # noqa: E501
            ("workflow_persistence_retry_jitter_ratio", 1.1, "must be a number between 0 and 1"),  # noqa: E501
            ("workflow_persistence_retry_warning_interval_seconds", -1, "must be a positive number of seconds"),  # noqa: E501
            ("workflow_persistence_retry_error_after_seconds", -1, "must be None or a non-negative number of seconds"),  # noqa: E501
        ],
    )
    async def test_gru_raises_on_invalid_workflow_persistence_retry_settings(
        self, kwarg: str, value: object, match: str
    ) -> None:
        kwargs: dict[str, Any] = {kwarg: value}
        with pytest.raises(ValueError, match=match):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
                **kwargs,
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("kwarg", "match"),
        [
            (
                "workflow_persistence_retry_delay_seconds",
                "must be a positive number of seconds",
            ),
            (
                "workflow_persistence_retry_max_delay_seconds",
                "must be a positive number of seconds",
            ),
            (
                "workflow_persistence_retry_backoff_multiplier",
                "must be a number greater than or equal to 1",
            ),
            (
                "workflow_persistence_retry_jitter_ratio",
                "must be a number between 0 and 1",
            ),
            (
                "workflow_persistence_retry_warning_interval_seconds",
                "must be a positive number of seconds",
            ),
            (
                "workflow_persistence_retry_error_after_seconds",
                "must be None or a non-negative number of seconds",
            ),
        ],
    )
    async def test_gru_rejects_boolean_workflow_persistence_retry_settings(
        self,
        kwarg: str,
        match: str,
    ) -> None:
        kwargs: dict[str, Any] = {kwarg: True}
        with pytest.raises(ValueError, match=match):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
                **kwargs,
            )

    @pytest.mark.asyncio
    async def test_gru_raises_when_workflow_persistence_retry_max_delay_is_below_initial_delay(
        self,
    ) -> None:
        with pytest.raises(
            ValueError,
            match=(
                "workflow_persistence_retry_max_delay_seconds must be greater "
                "than or equal to workflow_persistence_retry_delay_seconds"
            ),
        ):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
                workflow_persistence_retry_delay_seconds=2.0,
                workflow_persistence_retry_max_delay_seconds=1.0,
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_orchestration(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"

        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result1 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
            )

            assert result1.success
            assert result1.orchestration_id is not None
            await assert_orchestration_running(gru, result1.orchestration_id)

            result2 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
            )

            assert not result2.success
            assert result2.reason
            assert "Orchestration already running" in result2.reason

    @pytest.mark.asyncio
    async def test_gru_rejects_dict_inline_minion_config(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        from tests.assets.minions.two_steps.counter.with_file_config import (
            AssetMinion as FileConfigMinion,
        )
        from tests.assets.pipelines.emit_one.counter.default import (
            AssetPipeline as EmitOneCounterPipeline,
        )

        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                pipeline=EmitOneCounterPipeline,
                minion=FileConfigMinion,
                minion_config={"name": "dict"},
            )

            assert not result.success
            assert result.reason == (
                "Gru.start_orchestration: minion_config type is not supported. "
                "Supported user-declared types: (dataclass, msgspec.Struct)."
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_orchestration(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.stop_orchestration("dummy-orchestration-id")

            assert not result.success
            assert result.reason
            assert "No orchestration found with the given ID" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.default"
        pipeline_module_path = "tests.assets.pipelines.emit_one.record.default"

        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
            )

            assert not result.success
            assert result.reason
            assert "Incompatible minion and pipeline event types" in result.reason


class TestInvalidUsageDSL:
    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"

        directives: list[Directive] = [
            OrchestrationStart(
                pipeline=pipeline_module_path,
                minion="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStart(
                pipeline=pipeline_module_path,
                minion="tests.assets.minions.two_steps.counter.default",
                expect_success=False,
            ),
            WaitWorkflowCompletions(),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        directives: list[Directive] = [
            OrchestrationStop(id="dummy-orchestration-id", expect_success=False),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={},
        )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:

        directives: list[Directive] = [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.record.default",
                minion="tests.assets.minions.two_steps.counter.default",
                expect_success=False,
            ),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={},
        )
