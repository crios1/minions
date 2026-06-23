import asyncio
import contextlib
import sys
from collections.abc import Callable
from pathlib import Path
from textwrap import dedent
from typing import Protocol, cast

import pytest

from minions import Minion, Pipeline, minion_id, minion_step, pipeline_id
from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import WorkflowPersistenceFailurePolicy
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_spied_configed import AssetMinionConfig
from tests.assets.support.state_store_inmemory import InMemoryStateStore
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

MINION_COMPONENT_ID = "77777777-7777-4777-8777-77777777777a"
PIPELINE_COMPONENT_ID = "88888888-8888-4888-8888-88888888888b"
CONFIG_ID = "99999999-9999-4999-8999-99999999999c"


class _LoadedConfig(Protocol):
    name: str


class _ConfigurableMinion(Protocol):
    config: _LoadedConfig


class TestValidUsage:
    @pytest.mark.asyncio
    async def test_gru_accepts_none_logger_metrics_state_store(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(logger=None, state_store=None, metrics=None):
            pass

    @pytest.mark.asyncio
    async def test_gru_allows_create_and_immediate_shutdown(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()
        ):
            pass

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "policy",
        ["continue-on-failure", "idle-until-persisted"],
    )
    async def test_gru_accepts_workflow_persistence_failure_policy(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        policy: WorkflowPersistenceFailurePolicy,
    ) -> None:
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            workflow_persistence_failure_policy=policy,
        ):
            pass

    @pytest.mark.asyncio
    async def test_gru_accepts_workflow_persistence_retry_settings(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
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
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
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

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path
            )

            assert result.success
            assert result.orchestration_id in gru._minions_by_orchestration_id
            assert (
                gru._minions_by_orchestration_id[result.orchestration_id]._mn_minion_instance_id
                in gru._minion_tasks
            )

            await AssetMinion.wait_for_calls(
                expected={"step_1": 1, "step_2": 1},
                timeout=5.0,
            )

            await gru.stop_orchestration(result.orchestration_id)

    @pytest.mark.asyncio
    async def test_gru_start_stop_orchestration_from_classes(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        from tests.assets.minions.two_steps.counter.default import (
            AssetMinion as TwoStepCounterMinion,
        )
        from tests.assets.pipelines.emit_one.counter.default import (
            AssetPipeline as EmitOneCounterPipeline,
        )

        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics(),
        ) as gru:
            start_result = await gru.start_orchestration(
                pipeline=EmitOneCounterPipeline,
                minion=TwoStepCounterMinion,
            )

            assert start_result.success
            assert start_result.orchestration_id in gru._minions_by_orchestration_id
            assert (
                gru._minions_by_orchestration_id[
                    start_result.orchestration_id
                ]._mn_minion_instance_id
                in gru._minion_tasks
            )

            stop_result = await gru.stop_orchestration(start_result.orchestration_id)
            assert stop_result.success

    @pytest.mark.asyncio
    async def test_gru_start_orchestration_uses_attached_component_ids(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        @pipeline_id(PIPELINE_COMPONENT_ID)
        class LifecyclePipeline(Pipeline[SimpleEvent]):
            async def produce_event(self) -> SimpleEvent:
                await asyncio.sleep(3600)
                return SimpleEvent(timestamp=0)

        @minion_id(MINION_COMPONENT_ID)
        class LifecycleMinion(Minion[SimpleEvent, SimpleContext]):
            @minion_step
            async def step_1(self) -> None:
                self.context.step1 = "step1"

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=logger,
            metrics=NoOpMetrics(),
        ) as gru:
            start_result = await gru.start_orchestration(
                pipeline=LifecyclePipeline,
                minion=LifecycleMinion,
            )

            assert start_result.success
            assert start_result.orchestration_id is not None
            assert len(start_result.orchestration_id) == 44
            assert start_result.orchestration_id in gru._minions_by_orchestration_id
            assert PIPELINE_COMPONENT_ID in gru._pipelines
            assert logger.has_log(
                "Orchestration started",
                log_kwargs={
                    "orchestration_id": start_result.orchestration_id,
                    "minion_id": MINION_COMPONENT_ID,
                    "pipeline_id": PIPELINE_COMPONENT_ID,
                    "minion_config_id": "",
                },
            )

            stop_result = await gru.stop_orchestration(start_result.orchestration_id)
            assert stop_result.success

    @pytest.mark.asyncio
    async def test_gru_start_orchestration_uses_attached_component_and_config_ids(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        package_dir = tmp_path / "durable_app"
        package_dir.mkdir()
        (package_dir / "__init__.py").write_text("")
        (package_dir / "pipeline.py").write_text(
            dedent(
                f"""\
                import asyncio
                from minions import Pipeline, pipeline_id
                from tests.assets.events.simple import SimpleEvent

                @pipeline_id({PIPELINE_COMPONENT_ID!r})
                class DurablePipeline(Pipeline[SimpleEvent]):
                    async def produce_event(self) -> SimpleEvent:
                        await asyncio.sleep(3600)
                        return SimpleEvent(timestamp=0)
                """
            )
        )
        (package_dir / "minion.py").write_text(
            dedent(
                f"""\
                import tomllib
                from dataclasses import dataclass
                from pathlib import Path

                from minions import Minion, minion_id, minion_step
                from tests.assets.contexts.simple import SimpleContext
                from tests.assets.events.simple import SimpleEvent

                @dataclass
                class DurableConfig:
                    name: str

                @minion_id({MINION_COMPONENT_ID!r})
                class DurableMinion(Minion[SimpleEvent, SimpleContext]):
                    config: DurableConfig

                    async def load_config(self, config_path: str) -> DurableConfig:
                        parsed = tomllib.loads(Path(config_path).read_text())
                        return DurableConfig(name=parsed['config']['name'])

                    @minion_step
                    async def step_1(self) -> None:
                        self.context.step1 = self.config.name
                """
            )
        )
        config_path = tmp_path / "minion.toml"
        config_path.write_text(
            f'_minions_config_id = "{CONFIG_ID}"\n\n'
            '[config]\nname = "alpha"\n'
        )
        monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
        for module_name in ("durable_app.minion", "durable_app.pipeline"):
            sys.modules.pop(module_name, None)

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=logger,
            metrics=NoOpMetrics(),
        ) as gru:
            start_result = await gru.start_orchestration(
                pipeline="durable_app.pipeline",
                minion="durable_app.minion",
                minion_config_path=str(config_path),
            )

            assert start_result.success
            assert start_result.orchestration_id is not None
            orchestration_id = start_result.orchestration_id
            assert len(orchestration_id) == 44
            assert orchestration_id in gru._minions_by_orchestration_id
            minion = cast(_ConfigurableMinion, gru._minions_by_orchestration_id[orchestration_id])
            assert minion.config.name == "alpha"
            assert logger.has_log(
                "Orchestration started",
                log_kwargs={
                    "orchestration_id": orchestration_id,
                    "minion_id": MINION_COMPONENT_ID,
                    "pipeline_id": PIPELINE_COMPONENT_ID,
                    "minion_config_id": CONFIG_ID,
                },
            )

            stop_result = await gru.stop_orchestration(orchestration_id)
            assert stop_result.success

    @pytest.mark.asyncio
    async def test_gru_resumes_moved_id_bearing_source_and_config_artifacts(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        monkeypatch: pytest.MonkeyPatch,
        tmp_path: Path,
    ) -> None:
        moved_package_dir = tmp_path / "moved_app"
        moved_package_dir.mkdir()
        (moved_package_dir / "__init__.py").write_text("")
        (moved_package_dir / "pipeline.py").write_text(
            dedent(
                f"""\
                import asyncio
                from minions import Pipeline, pipeline_id
                from tests.assets.events.simple import SimpleEvent

                @pipeline_id({PIPELINE_COMPONENT_ID!r})
                class MovedPipeline(Pipeline[SimpleEvent]):
                    async def produce_event(self) -> SimpleEvent:
                        await asyncio.sleep(3600)
                        return SimpleEvent(timestamp=0)
                """
            )
        )
        (moved_package_dir / "minion.py").write_text(
            dedent(
                f"""\
                import tomllib
                from dataclasses import dataclass
                from pathlib import Path

                from minions import Minion, minion_id, minion_step
                from tests.assets.contexts.simple import SimpleContext
                from tests.assets.events.simple import SimpleEvent

                @dataclass
                class MovedConfig:
                    name: str

                @minion_id({MINION_COMPONENT_ID!r})
                class MovedMinion(Minion[SimpleEvent, SimpleContext]):
                    config: MovedConfig

                    async def load_config(self, config_path: str) -> MovedConfig:
                        parsed = tomllib.loads(Path(config_path).read_text())
                        return MovedConfig(name=parsed['config']['name'])

                    @minion_step
                    async def step_1(self) -> None:
                        raise AssertionError('step_1 should not replay after resume')

                    @minion_step
                    async def step_2(self) -> None:
                        self.context.step2 = self.config.name
                """
            )
        )
        moved_config_path = tmp_path / "renamed" / "minion.toml"
        moved_config_path.parent.mkdir()
        moved_config_path.write_text(
            f'_minions_config_id = "{CONFIG_ID}"\n\n'
            '[config]\nname = "moved-alpha"\n'
        )
        monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
        for module_name in ("moved_app.minion", "moved_app.pipeline"):
            sys.modules.pop(module_name, None)

        expected_orchestration_id = Gru._make_orchestration_id(
            pipeline_id=PIPELINE_COMPONENT_ID,
            minion_id=MINION_COMPONENT_ID,
            minion_config_id=CONFIG_ID,
        )
        logger = InMemoryLogger()
        state_store = InMemoryStateStore(logger=logger)
        await state_store._mn_serialize_and_save_context(
            MinionWorkflowContext(
                orchestration_id=expected_orchestration_id,
                workflow_id="wf-moved-resume",
                event=SimpleEvent(timestamp=123),
                context=SimpleContext(step1="already-complete"),
                context_cls=SimpleContext,
                next_step_index=1,
            )
        )

        async with gru_factory(
            state_store=state_store,
            logger=logger,
            metrics=NoOpMetrics(),
        ) as gru:
            start_result = await gru.start_orchestration(
                pipeline="moved_app.pipeline",
                minion="moved_app.minion",
                minion_config_path=str(moved_config_path),
            )

            assert start_result.success
            assert start_result.orchestration_id == expected_orchestration_id
            orchestration_id = start_result.orchestration_id
            assert orchestration_id is not None
            assert await logger.wait_for_log(
                "Workflow resumed",
                log_kwargs={
                    "workflow_id": "wf-moved-resume",
                    "orchestration_id": expected_orchestration_id,
                    "minion_id": MINION_COMPONENT_ID,
                    "pipeline_id": PIPELINE_COMPONENT_ID,
                    "minion_config_id": CONFIG_ID,
                },
                timeout=1.0,
                poll_interval=0.01,
            )
            assert await logger.wait_for_log(
                "Workflow succeeded",
                log_kwargs={
                    "workflow_id": "wf-moved-resume",
                    "orchestration_id": expected_orchestration_id,
                    "minion_id": MINION_COMPONENT_ID,
                    "pipeline_id": PIPELINE_COMPONENT_ID,
                    "minion_config_id": CONFIG_ID,
                },
                timeout=1.0,
                poll_interval=0.01,
            )
            assert await state_store.get_contexts_for_orchestration(expected_orchestration_id) == []

            stop_result = await gru.stop_orchestration(orchestration_id)
            assert stop_result.success

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
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
        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
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
            assert len(gru._pipelines) >= 3

            # Expect three distinct resource classes started
            assert len(gru._resources) >= 3

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
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
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

        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
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
            assert len(gru._pipelines) == 1

            # resource should be shared across minions
            assert len(gru._resources) == 1

            await Simple1ResourceMinion.wait_for_calls(
                expected={"step_1": 3, "step_2": 3},
                timeout=5.0,
            )

            # stop minions and assert cleanup
            assert r1.orchestration_id is not None
            await gru.stop_orchestration(r1.orchestration_id)
            assert len(gru._pipelines) == 1
            assert r2.orchestration_id is not None
            await gru.stop_orchestration(r2.orchestration_id)
            assert len(gru._pipelines) == 1
            assert r3.orchestration_id is not None
            await gru.stop_orchestration(r3.orchestration_id)

            # after all stopped, pipeline and resources cleaned
            assert len(gru._pipelines) == 0
            assert len(gru._resources) == 0

    @pytest.mark.asyncio
    async def test_gru_start_orchestration_shutdown_without_stop(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        ) as gru:
            result = await gru.start_orchestration(
                pipeline="tests.assets.pipelines.emit_one.simple.default",
                minion="tests.assets.minions.two_steps.simple.default",
            )

            assert result.success
            assert result.orchestration_id in gru._minions_by_orchestration_id
            assert (
                gru._minions_by_orchestration_id[result.orchestration_id]._mn_minion_instance_id
                in gru._minion_tasks
            )

    @pytest.mark.asyncio
    async def test_gru_binds_file_config_to_minion(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        tests_dir: Path,
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.counter.with_file_config"
        pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
        config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

        logger = InMemoryLogger()
        from tests.assets.minions.two_steps.counter.with_file_config import (
            AssetMinion as FileConfigMinion,
        )

        FileConfigMinion.enable_spy()
        FileConfigMinion.reset_spy()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics(),
        ) as gru:
            result = await gru.start_orchestration(
                minion=minion_module_path,
                minion_config_path=config_path,
                pipeline=pipeline_module_path,
            )

            assert result.success
            assert result.orchestration_id is not None

            await FileConfigMinion.wait_for_calls(
                expected={"step_1": 1, "step_2": 1},
                timeout=5.0
            )

            minion = gru._minions_by_orchestration_id[result.orchestration_id]
            assert isinstance(minion, FileConfigMinion)
            assert isinstance(minion.config, AssetMinionConfig)
            assert minion.config.name == "alpha"

            await gru.stop_orchestration(result.orchestration_id)

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "inline_config_kind",
        ["dataclass", "struct"],
    )
    async def test_gru_loads_inline_minion_config_from_classes(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
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
        async with gru_factory(
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

            minion = gru._minions_by_orchestration_id[result.orchestration_id]
            assert isinstance(minion, InlineConfigMinion)
            assert minion.config is inline_config

            stop = await gru.stop_orchestration(result.orchestration_id)
            assert stop.success

    @pytest.mark.asyncio
    async def test_minion_and_pipeline_share_resource_dependency(
        self,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        minion_module_path = "tests.assets.minions.two_steps.simple.with_simple_resource"
        pipeline_module_path = (
            "tests.assets.pipelines.emit_one.simple.with_simple_resource"
        )
        logger = InMemoryLogger()
        async with gru_factory(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        ) as gru:
            r1 = await gru.start_orchestration(
                pipeline=pipeline_module_path,
                minion=minion_module_path
            )

            assert r1.success

            assert len(gru._pipelines) == 1
            assert len(gru._resources) == 1

            assert isinstance(r1.orchestration_id, str)
            await gru.stop_orchestration(r1.orchestration_id)

            assert len(gru._pipelines) == 0
            assert len(gru._resources) == 0

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
            logger,
            metrics,
            state_store,
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
            logger,
            metrics,
            state_store,
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
            logger,
            metrics,
            state_store,
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
            logger,
            metrics,
            state_store,
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
            logger,
            metrics,
            state_store,
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
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline_module_path: 1},
        )
