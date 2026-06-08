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
from tests.support.gru_scenario import (
    Directive,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    WaitWorkflowCompletions,
    run_gru_scenario,
)


class TestInvalidUsage:
    # Legacy/manual baseline during DSL confidence window.
    # Orchestration-invalid coverage should be added/updated in `TestInvalidUsageDSL`
    # and `TestInvalidUsageUsingNewAssetsDSL`.
    @pytest.mark.asyncio
    async def test_gru_raises_on_direct_instantiation(self) -> None:
        with pytest.raises(RuntimeError):
            Gru(
                loop=asyncio.get_running_loop(),
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics()
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_multiple_instances(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
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
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        # TODO:
        # - start 2 minions with the same name (would need to start different
        #   minion but give the same name)
        # - stop minion by id => and get error as a value
        # TODO: but actually runs with events be created and stuff?

        print("--------- start problematic test ---------")

        minion_modpath = "tests.assets.minions.two_steps.simple.basic"
        pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result1 = await gru.start_orchestration(
                minion=minion_modpath,
                pipeline=pipeline_modpath
            )

            print(result1)

            assert result1.success
            assert result1.orchestration_id is not None
            assert result1.orchestration_id in gru._minions_by_orchestration_id
            assert (
                gru._minions_by_orchestration_id[result1.orchestration_id]._mn_minion_instance_id
                in gru._minion_tasks
            )

            result2 = await gru.start_orchestration(
                minion=minion_modpath,
                pipeline=pipeline_modpath
            )

            print(result2)

            assert not result2.success
            assert result2.reason
            assert "Orchestration already running" in result2.reason

    @pytest.mark.asyncio
    async def test_gru_rejects_dict_inline_minion_config(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        from tests.assets.minions.two_steps.counter.uses_config import ConfigMinion
        from tests.assets.pipelines.emit1.counter.emit_1 import Emit1Pipeline

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=ConfigMinion,
                pipeline=Emit1Pipeline,
                minion_config={"name": "dict"},
            )

            assert not result.success
            assert result.reason == (
                "Gru.start_orchestration: minion_config type must be a dataclass "
                "or msgspec Struct type."
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_orchestration(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.stop_orchestration("mock")

            print(result)

            assert not result.success
            assert result.reason
            assert "No orchestration found with the given ID" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        minion_modpath = "tests.assets.minions.two_steps.simple.basic"
        pipeline_modpath = "tests.assets.pipelines.simple.record_event"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_modpath,
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
    async def test_gru_returns_error_when_starting_running_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

        directives: list[Directive] = [
            OrchestrationStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline=pipeline_modpath,
            ),
            OrchestrationStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline=pipeline_modpath,
                expect_success=False,
            ),
            WaitWorkflowCompletions(),
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
    async def test_gru_returns_error_when_stopping_nonexistant_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        directives: list[Directive] = [
            OrchestrationStop(id="mock", expect_success=False),
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
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:

        directives: list[Directive] = [
            OrchestrationStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline="tests.assets.pipelines.types.record_event",
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


class TestInvalidUsageUsingNewAssetsDSL:
    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

        directives: list[Directive] = [
            OrchestrationStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline=pipeline_modpath,
            ),
            OrchestrationStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline=pipeline_modpath,
                expect_success=False,
            ),
            WaitWorkflowCompletions(),
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
    async def test_gru_returns_error_when_stopping_nonexistant_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        directives: list[Directive] = [
            OrchestrationStop(id="mock", expect_success=False),
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
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:

        directives: list[Directive] = [
            OrchestrationStart(
                minion="tests.assets.minions.two_steps.counter.basic",
                pipeline="tests.assets.pipelines.types.record_event",
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
    # Legacy/manual baseline during DSL confidence window.
    # Orchestration-invalid coverage should be added/updated in
    # `TestInvalidUsageUsingNewAssetsDSL`.
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
    async def test_gru_raises_on_multiple_instances(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
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
    async def test_gru_raises_on_invalid_logger_param(self, bad_logger: object) -> None:
        with pytest.raises(TypeError):
            await Gru.create(
                logger=cast(Any, bad_logger),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics", [123, "invalid"])
    async def test_gru_raises_on_invalid_metrics_param(self, bad_metrics: object) -> None:
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=cast(Any, bad_metrics),
                state_store=NoOpStateStore(),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_state_store", [123, "invalid"])
    async def test_gru_raises_on_invalid_state_store_param(self, bad_state_store: object) -> None:
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=cast(Any, bad_state_store),
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_orchestration(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        minion_modpath = "tests.assets.minions.two_steps.counter.basic"
        pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result1 = await gru.start_orchestration(
                minion=minion_modpath,
                pipeline=pipeline_modpath,
            )

            assert result1.success
            assert result1.orchestration_id is not None
            assert result1.orchestration_id in gru._minions_by_orchestration_id
            assert (
                gru._minions_by_orchestration_id[result1.orchestration_id]._mn_minion_instance_id
                in gru._minion_tasks
            )

            result2 = await gru.start_orchestration(
                minion=minion_modpath,
                pipeline=pipeline_modpath,
            )

            assert not result2.success
            assert result2.reason
            assert "Orchestration already running" in result2.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_orchestration(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.stop_orchestration("mock")

            assert not result.success
            assert result.reason
            assert "No orchestration found" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(
        self, gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]]
    ) -> None:
        minion_modpath = "tests.assets.minions.two_steps.counter.basic"
        pipeline_modpath = "tests.assets.pipelines.types.record_event"

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_modpath,
                pipeline=pipeline_modpath,
            )

            assert not result.success
            assert result.reason
            assert "Incompatible minion and pipeline event types" in result.reason
