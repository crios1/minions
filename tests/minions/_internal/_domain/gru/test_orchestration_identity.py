import contextlib
import sys
from collections.abc import Callable
from pathlib import Path
from textwrap import dedent

import pytest

from minions import Minion, minion_id, minion_step, pipeline_id
from minions._internal._domain.gru import Gru
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.pipeline_triggered import TriggeredPipeline
from tests.minions._internal._domain.gru.assertions import assert_orchestration_running

MINION_COMPONENT_ID = "77777777-7777-4777-8777-77777777777a"
PIPELINE_COMPONENT_ID = "88888888-8888-4888-8888-88888888888b"
CONFIG_ID = "99999999-9999-4999-8999-99999999999c"


@pytest.mark.asyncio
async def test_class_based_start_uses_distinct_fallback_identities_for_same_module_classes(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
):
    class SharedPipeline(TriggeredPipeline[EmptyEvent]):
        async def produce_event(self) -> EmptyEvent:
            return EmptyEvent()

    class FirstMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self) -> None:
            return

    class SecondMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_1(self) -> None:
            return

    async with managed_gru_context(
        state_store=NoOpStateStore(),
        logger=logger,
        metrics=NoOpMetrics(),
    ) as gru:
        first = await gru.start_orchestration(SharedPipeline, FirstMinion)
        second = await gru.start_orchestration(SharedPipeline, SecondMinion)

        assert first.success
        assert second.success
        assert first.orchestration_id is not None
        assert second.orchestration_id is not None
        assert first.orchestration_id != second.orchestration_id
        snapshot = await gru.runtime_state_snapshot()
        assert snapshot.orchestrations == {
            first.orchestration_id,
            second.orchestration_id,
        }
        assert snapshot.pipelines == frozenset(
            {f"{SharedPipeline.__module__}.{SharedPipeline.__name__}"}
        )

        assert (await gru.stop_orchestration(first.orchestration_id)).success
        assert (await gru.stop_orchestration(second.orchestration_id)).success


@pytest.mark.asyncio
async def test_start_orchestration_uses_attached_component_ids(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
):
    @pipeline_id(PIPELINE_COMPONENT_ID)
    class IdentifiedPipeline(TriggeredPipeline[SimpleEvent]):
        async def produce_event(self) -> SimpleEvent:
            return SimpleEvent(timestamp=0)

    @minion_id(MINION_COMPONENT_ID)
    class LifecycleMinion(Minion[SimpleEvent, SimpleContext]):
        @minion_step
        async def step_1(self) -> None:
            self.context.step1 = "step1"

    async with managed_gru_context(
        state_store=NoOpStateStore(),
        logger=logger,
        metrics=NoOpMetrics(),
    ) as gru:
        start_result = await gru.start_orchestration(
            pipeline=IdentifiedPipeline,
            minion=LifecycleMinion,
        )

        assert start_result.success
        assert start_result.orchestration_id is not None
        assert start_result.orchestration_id == Gru._make_orchestration_id(
            pipeline_id=PIPELINE_COMPONENT_ID,
            minion_id=MINION_COMPONENT_ID,
            minion_config_id="",
        )
        await assert_orchestration_running(gru, start_result.orchestration_id)
        assert PIPELINE_COMPONENT_ID in (await gru.runtime_state_snapshot()).pipelines
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
async def test_start_orchestration_uses_attached_component_and_config_ids(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    package_dir = tmp_path / "durable_app"
    package_dir.mkdir()
    (package_dir / "__init__.py").write_text("")
    (package_dir / "pipeline.py").write_text(
        dedent(
            f"""\
            from minions import pipeline_id
            from tests.assets.events.simple import SimpleEvent
            from tests.assets.support.pipeline_triggered import TriggeredPipeline

            @pipeline_id({PIPELINE_COMPONENT_ID!r})
            class DurablePipeline(TriggeredPipeline[SimpleEvent]):
                async def produce_event(self) -> SimpleEvent:
                    return SimpleEvent(timestamp=0)
            """
        )
    )
    (package_dir / "minion.py").write_text(
        dedent(
            f"""\
            import asyncio
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
                    contents = await asyncio.to_thread(Path(config_path).read_text)
                    parsed = tomllib.loads(contents)
                    return DurableConfig(name=parsed['config']['name'])

                @minion_step
                async def step_1(self) -> None:
                    self.context.step1 = self.config.name
            """
        )
    )
    config_path = tmp_path / "minion.toml"
    config_path.write_text(f'_minions_config_id = "{CONFIG_ID}"\n\n[config]\nname = "alpha"\n')
    monkeypatch.setattr(sys, "path", [str(tmp_path), *sys.path])
    for module_name in ("durable_app.minion", "durable_app.pipeline"):
        sys.modules.pop(module_name, None)

    async with managed_gru_context(
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
        assert orchestration_id == Gru._make_orchestration_id(
            pipeline_id=PIPELINE_COMPONENT_ID,
            minion_id=MINION_COMPONENT_ID,
            minion_config_id=CONFIG_ID,
        )
        await assert_orchestration_running(gru, orchestration_id)
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
