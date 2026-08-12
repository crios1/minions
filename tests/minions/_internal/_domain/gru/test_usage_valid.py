import contextlib
from collections.abc import Callable
from pathlib import Path

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import (
    WorkflowFailurePolicy,
    WorkflowPersistenceFailurePolicy,
)
from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import (
    assert_orchestration_running,
    assert_runtime_component_counts_at_least,
    assert_runtime_component_counts_exact,
)
from tests.support.gru_scenario import (
    AfterWorkflowStepStarts,
    Directive,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
    run_gru_scenario,
)


class TestValidUsage:
    @pytest.mark.asyncio
    async def test_gru_accepts_none_logger_metrics_state_store(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with managed_gru_context(logger=None, state_store=None, metrics=None):
            pass

    @pytest.mark.asyncio
    async def test_gru_allows_create_and_immediate_shutdown(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with managed_gru_context(
            state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()
        ):
            pass

    @pytest.mark.asyncio
    async def test_gru_accepts_workflow_failure_policy(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        policy: WorkflowFailurePolicy = "delete"
        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            workflow_failure_policy=policy,
        ) as gru:
            assert gru._workflow_failure_policy == "delete"

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "policy",
        ["continue-on-failure", "idle-until-persisted"],
    )
    async def test_gru_accepts_workflow_persistence_failure_policy(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        policy: WorkflowPersistenceFailurePolicy,
    ) -> None:
        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            workflow_persistence_failure_policy=policy,
        ):
            pass

    @pytest.mark.asyncio
    async def test_gru_accepts_workflow_persistence_retry_settings(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            workflow_persistence_retry_delay_seconds=0.25,
            workflow_persistence_retry_max_delay_seconds=2.0,
            workflow_persistence_retry_backoff_multiplier=1.5,
            workflow_persistence_retry_jitter_ratio=0.2,
            workflow_persistence_retry_warning_interval_seconds=5.0,
            workflow_persistence_retry_error_after_seconds=None,
        ):
            pass

    @pytest.mark.asyncio
    async def test_gru_start_stop_orchestration(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.simple.default"
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.simple.default"
        )

        from tests.assets.minions.two_steps.simple.default import AssetMinion
        from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

        AssetMinion.enable_spy()
        AssetMinion.reset_spy()
        AssetPipeline.configure_gate(expected_subs=1)

        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path
            )

            assert result.success
            assert result.orchestration_id is not None
            await assert_orchestration_running(gru, result.orchestration_id)

            await AssetMinion.wait_for_calls(
                expected={"step_1": 1, "step_2": 1},
                timeout=5.0,
            )

            await gru.stop_orchestration(result.orchestration_id)

    @pytest.mark.asyncio
    async def test_gru_start_stop_orchestration_from_classes(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        from tests.assets.minions.two_steps.counter.default import (
            AssetMinion as TwoStepCounterMinion,
        )
        from tests.assets.pipelines.emit_one.counter.default import (
            AssetPipeline as EmitOneCounterPipeline,
        )

        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            start_result = await gru.start_orchestration(
                pipeline=EmitOneCounterPipeline,
                minion=TwoStepCounterMinion,
            )

            assert start_result.success
            assert start_result.orchestration_id is not None
            await assert_orchestration_running(gru, start_result.orchestration_id)

            stop_result = await gru.stop_orchestration(start_result.orchestration_id)
            assert stop_result.success

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        """
        Start three minions each with their own pipeline and their own Resource type
        so there is no sharing of pipelines or resources between minions.
        """
        minion1 = "tests.assets.minions.two_steps.simple.with_simple_resource"
        minion2 = "tests.assets.minions.two_steps.simple.with_simple_b_resource"
        minion3 = "tests.assets.minions.two_steps.simple.with_simple_c_resource"

        pipeline1 = "tests.assets.pipelines.emit_one.simple.default"
        pipeline2 = "tests.assets.pipelines.emit_one.simple.default_b"
        pipeline3 = "tests.assets.pipelines.emit_one.simple.default_c"
        async with managed_gru_context(
            state_store=state_store,
            logger=logger,
            metrics=metrics,
        ) as gru:
            from tests.assets.minions.two_steps.simple.with_simple_b_resource import (
                AssetMinion as Simple2ResourceMinion,
            )
            from tests.assets.minions.two_steps.simple.with_simple_c_resource import (
                AssetMinion as Simple3ResourceMinion,
            )
            from tests.assets.minions.two_steps.simple.with_simple_resource import (
                AssetMinion as Simple1ResourceMinion,
            )

            for cls in (
                Simple1ResourceMinion,
                Simple2ResourceMinion,
                Simple3ResourceMinion,
            ):
                cls.enable_spy()
                cls.reset_spy()

            r1 = await gru.start_orchestration(minion=minion1, pipeline=pipeline1)
            r2 = await gru.start_orchestration(minion=minion2, pipeline=pipeline2)
            r3 = await gru.start_orchestration(minion=minion3, pipeline=pipeline3)

            assert r1.success and r2.success and r3.success

            # Expect three distinct pipeline IDs
            await assert_runtime_component_counts_at_least(gru, pipelines=3)

            # Expect three distinct resource classes started
            await assert_runtime_component_counts_at_least(gru, resources=3)

            await Simple1ResourceMinion.wait_for_calls(
                expected={"step_1": 1, "step_2": 1}, timeout=5.0
            )
            await Simple2ResourceMinion.wait_for_calls(
                expected={"step_1": 1, "step_2": 1}, timeout=5.0
            )
            await Simple3ResourceMinion.wait_for_calls(
                expected={"step_1": 1, "step_2": 1}, timeout=5.0
            )

            # stop them
            assert r1.orchestration_id is not None
            await gru.stop_orchestration(r1.orchestration_id)
            assert r2.orchestration_id is not None
            await gru.stop_orchestration(r2.orchestration_id)
            assert r3.orchestration_id is not None
            await gru.stop_orchestration(r3.orchestration_id)

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
        tests_dir: Path,
    ) -> None:
        """
        Start three minions that share the same pipeline and a single Resource type.
        Verify pipeline and resource are shared and cleaned up after stopping all minions.
        """
        minion_module_path = "tests.assets.minions.two_steps.simple.with_simple_resource"
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.simple.default"
        )
        from tests.assets.minions.two_steps.simple.with_simple_resource import (
            AssetMinion as Simple1ResourceMinion,
        )
        from tests.assets.pipelines.emit_one.simple.default import AssetPipeline

        Simple1ResourceMinion.enable_spy()
        Simple1ResourceMinion.reset_spy()
        AssetPipeline.configure_gate(expected_subs=3)

        # TODO: I'm testing resource sharing between minions spawned from the
        # same minion class but different configs.
        # I should also test the case where I spawn from separate minion
        # classes/files.
        cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
        cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
        cfg3 = str(tests_dir / "assets" / "config/minions/c.toml")

        # TODO: consider refactoring gru to have the kwargs be classes instead of instances
        # it might be cleaner and then the user wont have to manually wire things like this
        # and cuz then gru can handle instantiation and startup
        # but what if the user wants to bring their own and instantiate with parameters?
        # ask copilot
        # !! will have to do the update across this whole test file !!

        async with managed_gru_context(
            state_store=state_store,
            logger=logger,
            metrics=metrics,
        ) as gru:
            r1 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
                minion_config_path=cfg1,
            )
            r2 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
                minion_config_path=cfg2,
            )
            r3 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path,
                minion_config_path=cfg3,
            )

            assert r1.success and r2.success and r3.success

            # pipeline should be shared (single id)
            await assert_runtime_component_counts_exact(gru, pipelines=1)

            # resource should be shared across minions
            await assert_runtime_component_counts_exact(gru, resources=1)

            await Simple1ResourceMinion.wait_for_calls(
                expected={"step_1": 3, "step_2": 3},
                timeout=5.0,
            )

            # stop minions and assert cleanup
            assert r1.orchestration_id is not None
            await gru.stop_orchestration(r1.orchestration_id)
            await assert_runtime_component_counts_exact(gru, pipelines=1)
            assert r2.orchestration_id is not None
            await gru.stop_orchestration(r2.orchestration_id)
            await assert_runtime_component_counts_exact(gru, pipelines=1)
            assert r3.orchestration_id is not None
            await gru.stop_orchestration(r3.orchestration_id)

            # after all stopped, pipeline and resources cleaned
            await assert_runtime_component_counts_exact(gru, pipelines=0, resources=0)

    @pytest.mark.asyncio
    async def test_gru_start_orchestration_shutdown_without_stop(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                pipeline="tests.assets.pipelines.emit_one.simple.default",
                minion="tests.assets.minions.two_steps.simple.default",
            )

            assert result.success
            assert result.orchestration_id is not None
            await assert_orchestration_running(gru, result.orchestration_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "inline_config_kind",
        ["dataclass", "struct"],
    )
    async def test_gru_loads_inline_minion_config_from_classes(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        inline_config_kind: str,
    ) -> None:
        from tests.assets.minions.one_step.counter.with_inline_config import (
            AssetMinion as InlineConfigMinion,
        )
        from tests.assets.minions.one_step.counter.with_inline_config import (
            InlineDataclassConfig,
            InlineStructConfig,
        )
        from tests.assets.pipelines.emit_one.counter.default import (
            AssetPipeline as EmitOneCounterPipeline,
        )

        inline_config = (
            InlineDataclassConfig(name="dataclass")
            if inline_config_kind == "dataclass"
            else InlineStructConfig(name="struct")
        )
        async with managed_gru_context(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                pipeline=EmitOneCounterPipeline,
                minion=InlineConfigMinion,
                minion_config=inline_config,
            )

            assert result.success
            assert result.orchestration_id is not None

            minion = gru._orchestrations[result.orchestration_id].minion
            assert isinstance(minion, InlineConfigMinion)
            assert minion.config is inline_config

            stop = await gru.stop_orchestration(result.orchestration_id)
            assert stop.success

    @pytest.mark.asyncio
    async def test_minion_and_pipeline_share_resource_dependency(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.simple.with_simple_resource"
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.simple.with_simple_resource"
        )
        async with managed_gru_context(
            state_store=state_store,
            logger=logger,
            metrics=metrics,
        ) as gru:
            r1 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path
            )

            assert r1.success

            await assert_runtime_component_counts_exact(gru, pipelines=1, resources=1)

            assert isinstance(r1.orchestration_id, str)
            await gru.stop_orchestration(r1.orchestration_id)

            await assert_runtime_component_counts_exact(gru, pipelines=0, resources=0)

    # TODO: I need tests for gru's default usages to ensure i stay version 1.x.x compliant


class TestValidUsageDSL:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("minion_module_path", "pipeline_module_path"),
        [
            (
                "tests.assets.minions.user_guarantees.persisted_dataclass",
                "tests.assets.pipelines.user_guarantees.persisted_dataclass",
            ),
            (
                "tests.assets.minions.user_guarantees.persisted_msgspec",
                "tests.assets.pipelines.user_guarantees.persisted_msgspec",
            ),
        ],
    )
    async def test_user_guarantee_persisted_event_and_context_shapes_resume(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
        minion_module_path: str,
        pipeline_module_path: str,
    ) -> None:
        start_1 = OrchestrationStart(pipeline=pipeline_module_path, minion=minion_module_path)
        start_2 = OrchestrationStart(pipeline=pipeline_module_path, minion=minion_module_path)
        directives: list[Directive] = [
            start_1,
            AfterWorkflowStepStarts(
                expected={start_1: {"step_1": 1}},
                directive=OrchestrationStop(id=start_1, expect_success=True),
            ),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    persistence={start_1: 1},
                    workflow_steps={start_1: {"step_1": 1}},
                    workflow_steps_mode="exact",
                ),
            ),
            start_2,
            WaitWorkflowCompletions(),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    resolutions={start_2: {"succeeded": 2, "failed": 0, "aborted": 0}},
                    workflow_steps={start_2: {"step_1": 2}},
                    workflow_steps_mode="exact",
                ),
            ),
            OrchestrationStop(id=start_2, expect_success=True),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )

    @pytest.mark.asyncio
    async def test_gru_start_stop_orchestration(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        start = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.default",
        )

        directives: list[Directive] = [
            start,
            WaitWorkflowCompletions(workflow_steps_mode="exact"),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    resolutions={
                        start: {"succeeded": 1, "failed": 0, "aborted": 0},
                    }
                ),
            ),
            OrchestrationStop(id=start, expect_success=True),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline1 = "tests.assets.pipelines.emit_one.counter.default"
        pipeline2 = "tests.assets.pipelines.emit_one.counter.default_b"
        pipeline3 = "tests.assets.pipelines.emit_one.counter.default_c"
        start_1 = OrchestrationStart(
            pipeline=pipeline1,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource",
        )
        start_2 = OrchestrationStart(
            pipeline=pipeline2,
            minion="tests.assets.minions.two_steps.counter.with_fixed_b_resource",
        )
        start_3 = OrchestrationStart(
            pipeline=pipeline3,
            minion="tests.assets.minions.two_steps.counter.with_fixed_c_resource",
        )

        directives: list[Directive] = [
            start_1,
            start_2,
            start_3,
            WaitWorkflowCompletions(workflow_steps_mode="exact"),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    resolutions={
                        start_1: {"succeeded": 1, "failed": 0, "aborted": 0},
                        start_2: {"succeeded": 1, "failed": 0, "aborted": 0},
                        start_3: {"succeeded": 1, "failed": 0, "aborted": 0},
                    }
                ),
            ),
            OrchestrationStop(id=start_1, expect_success=True),
            OrchestrationStop(id=start_2, expect_success=True),
            OrchestrationStop(id=start_3, expect_success=True),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={
                pipeline1: 1,
                pipeline2: 1,
                pipeline3: 1,
            },
        )

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.counter.after_three_subscribers"
        )
        start_1 = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource",
        )
        start_2 = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource_b",
        )
        start_3 = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource_c",
        )

        directives: list[Directive] = [
            start_1,
            start_2,
            start_3,
            WaitWorkflowCompletions(workflow_steps_mode="exact"),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    resolutions={
                        start_1: {"succeeded": 1, "failed": 0, "aborted": 0},
                        start_2: {"succeeded": 1, "failed": 0, "aborted": 0},
                        start_3: {"succeeded": 1, "failed": 0, "aborted": 0},
                    }
                ),
            ),
            OrchestrationStop(id=start_1, expect_success=True),
            OrchestrationStop(id=start_2, expect_success=True),
            OrchestrationStop(id=start_3, expect_success=True),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )

    @pytest.mark.asyncio
    async def test_minion_and_pipeline_share_resource_dependency(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.counter.with_fixed_resource"
        )
        start = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource",
        )

        directives: list[Directive] = [
            start,
            WaitWorkflowCompletions(workflow_steps_mode="exact"),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    resolutions={
                        start: {"succeeded": 1, "failed": 0, "aborted": 0},
                    }
                ),
            ),
            OrchestrationStop(id=start, expect_success=True),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )

    @pytest.mark.asyncio
    async def test_minion_and_pipeline_share_resource_without_duplicate_owner_ref(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.counter.with_fixed_resource"
        )
        first = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource",
        )
        second = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource_b",
        )
        await run_gru_scenario(
            gru,
            [
                first,
                second,
                WaitWorkflowCompletions(workflow_steps_mode="exact"),
                OrchestrationStop(id=first, expect_success=True),
                OrchestrationStop(id=second, expect_success=True),
                GruShutdown(expect_success=True),
            ],
            pipeline_event_counts={pipeline_module_path: 1},
        )

    @pytest.mark.asyncio
    async def test_gru_start_orchestration_shutdown_without_stop(
        self,
        gru: Gru,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
    ) -> None:
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        start = OrchestrationStart(
            pipeline=pipeline_module_path,
            minion="tests.assets.minions.two_steps.counter.default",
        )

        directives: list[Directive] = [
            start,
            WaitWorkflowCompletions(workflow_steps_mode="exact"),
            ExpectRuntime(
                expect=RuntimeExpectSpec(
                    resolutions={
                        start: {"succeeded": 1, "failed": 0, "aborted": 0},
                    }
                ),
            ),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )
