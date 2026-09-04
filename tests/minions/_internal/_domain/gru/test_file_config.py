import asyncio
import contextlib
import tomllib
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import pytest

from minions import Minion, minion_id, minion_step, pipeline_id
from minions._internal._domain.gru import Gru
from minions._internal._framework.logger import ERROR
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_spied_configed import AssetMinionConfig
from tests.assets.support.pipeline_triggered import TriggeredPipeline
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.minions._internal._domain.gru.assertions import assert_runtime_empty


@pytest.mark.asyncio
async def test_binds_loaded_config_to_minion(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    tests_dir: Path,
):
    minion_module_path = "tests.assets.minions.two_steps.counter.with_file_config"
    pipeline_module_path = "tests.assets.pipelines.emit_one.counter.default"
    config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")

    from tests.assets.minions.two_steps.counter.with_file_config import (
        AssetMinion as FileConfigMinion,
    )

    FileConfigMinion.enable_spy()
    FileConfigMinion.reset_spy()
    async with managed_gru_context(
        state_store=state_store,
        logger=logger,
        metrics=metrics,
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
            timeout=5.0,
        )

        minion = gru._orchestrations[result.orchestration_id].minion
        assert isinstance(minion, FileConfigMinion)
        assert isinstance(minion.config, AssetMinionConfig)
        assert minion.config.name == "alpha"

        await gru.stop_orchestration(result.orchestration_id)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("config_name", "contents", "expected_cause"),
    [
        ("missing.toml", None, "No such file or directory"),
        ("malformed.toml", "[", "Invalid initial character for a key part"),
        (
            "invalid-id.toml",
            '_minions_config_id = "not-a-uuid"\n',
            "config id must be a canonical UUID string",
        ),
    ],
    ids=("missing", "malformed", "invalid-identity"),
)
async def test_start_returns_failure_when_file_config_cannot_be_resolved(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    tmp_path: Path,
    config_name: str,
    contents: str | None,
    expected_cause: str,
):
    config_path = tmp_path / config_name
    if contents is not None:
        config_path.write_text(contents)

    async with managed_gru_context(
        state_store=state_store,
        logger=logger,
        metrics=metrics,
    ) as gru:
        result = await gru.start_orchestration(
            minion="tests.assets.minions.two_steps.counter.with_file_config",
            minion_config_path=str(config_path),
            pipeline="tests.assets.pipelines.emit_one.counter.default",
        )

        assert not result.success
        assert result.reason == "Minion config could not be resolved."
        assert result.cause is not None
        assert expected_cause in result.cause
        assert result.suggestion == (
            "Check that minion_config_path exists, is readable, and contains a valid "
            "optional _minions_config_id."
        )
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_returns_failure_when_file_config_cannot_be_read(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
):
    async def raise_permission_error(_config_path: str) -> tuple[str, str]:
        raise PermissionError("config access denied")

    async with managed_gru_context(
        state_store=state_store,
        logger=logger,
        metrics=metrics,
    ) as gru:
        monkeypatch.setattr(
            gru,
            "_resolve_file_config_path_and_identity",
            raise_permission_error,
        )
        result = await gru.start_orchestration(
            minion="tests.assets.minions.two_steps.counter.with_file_config",
            minion_config_path="unreadable.toml",
            pipeline="tests.assets.pipelines.emit_one.counter.default",
        )

        assert not result.success
        assert result.reason == "Minion config could not be resolved."
        assert result.cause == "config access denied"
        assert result.suggestion == (
            "Check that minion_config_path exists, is readable, and contains a valid "
            "optional _minions_config_id."
        )
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_propagates_cancellation_during_file_config_resolution(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
):
    async with managed_gru_context(
        state_store=state_store,
        logger=logger,
        metrics=metrics,
    ) as gru:
        async def cancelled_resolution(_config_path: str) -> tuple[str, str]:
            raise asyncio.CancelledError

        monkeypatch.setattr(
            gru,
            "_resolve_file_config_path_and_identity",
            cancelled_resolution,
        )

        with pytest.raises(asyncio.CancelledError):
            await gru.start_orchestration(
                minion="tests.assets.minions.two_steps.counter.with_file_config",
                minion_config_path="cancelled.toml",
                pipeline="tests.assets.pipelines.emit_one.counter.default",
            )

        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_start_without_load_config_override_returns_cause_and_cleans_up(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    tests_dir: Path,
):
    minion_module_path = "tests.assets.minions.two_steps.simple.default"
    pipeline_module_path = "tests.assets.pipelines.emit_one.simple.default"
    config_path = str(tests_dir / "assets" / "config" / "minions" / "a.toml")
    expected_cause = (
        "AssetMinion.load_config must be overridden to load file config into a "
        "dataclass or msgspec Struct instance."
    )
    expected_reason = (
        "tests.assets.minions.two_steps.simple.default.AssetMinion.startup failed"
    )

    async with managed_gru_context(
        state_store=state_store,
        logger=logger,
        metrics=metrics,
    ) as gru:
        result = await gru.start_orchestration(
            minion=minion_module_path,
            minion_config_path=config_path,
            pipeline=pipeline_module_path,
        )

        assert not result.success
        assert result.reason == expected_reason
        assert result.cause == expected_cause
        assert result.suggestion is None
        failure_log = logger.find_first_log(
            "Failed to start orchestration",
            min_level=ERROR,
        )
        assert failure_log is not None
        assert failure_log.kwargs["error_type"] == "MinionsError"
        assert failure_log.kwargs["error_message"] == expected_reason
        assert failure_log.kwargs["cause_error_type"] == "NotImplementedError"
        assert failure_log.kwargs["cause_error_message"] == expected_cause
        await assert_runtime_empty(gru)


@pytest.mark.asyncio
async def test_unchanged_id_resumes_persisted_workflow_with_reloaded_contents(
    managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
):
    @pipeline_id("88888888-8888-4888-8888-88888888888b")
    class SimpleEventPipeline(TriggeredPipeline[SimpleEvent]):
        async def produce_event(self) -> SimpleEvent:
            return SimpleEvent(timestamp=0)

    @dataclass
    class MarkerConfig:
        marker: str

    @dataclass
    class MarkerContext:
        marker_at_recording: str = ""
        marker_at_observation: str = ""

    observed_markers: list[str] = []
    observation_reached = asyncio.Event()
    allow_observation = asyncio.Event()

    @minion_id("77777777-7777-4777-8777-77777777777a")
    class ConfigObservingMinion(Minion[SimpleEvent, MarkerContext]):
        config: MarkerConfig

        async def load_config(self, config_path: str) -> MarkerConfig:
            contents = await asyncio.to_thread(Path(config_path).read_text)
            parsed = tomllib.loads(contents)
            return MarkerConfig(marker=parsed["config"]["marker"])

        @minion_step
        async def record_config_snapshot(self):
            self.context.marker_at_recording = self.config.marker

        @minion_step
        async def observe_config(self):
            observed_markers.append(self.config.marker)
            observation_reached.set()
            await allow_observation.wait()
            self.context.marker_at_observation = self.config.marker

    config_path = tmp_path / "config.toml"

    def write_config(marker: str):
        config_path.write_text(
            '_minions_config_id = "99999999-9999-4999-8999-99999999999c"\n\n'
            f'[config]\nmarker = "{marker}"\n'
        )

    write_config("alpha")

    async with managed_gru_context(
        state_store=state_store,
        logger=logger,
        metrics=metrics,
    ) as gru:
        # File config requires module paths, so map these fake paths to local classes.
        fake_minion_module_path = "config_reload.minion"
        fake_pipeline_module_path = "config_reload.pipeline"

        def resolve_minion_class(module_path: str) -> type[ConfigObservingMinion]:
            assert module_path == fake_minion_module_path
            return ConfigObservingMinion

        def resolve_pipeline_class(module_path: str) -> type[SimpleEventPipeline]:
            assert module_path == fake_pipeline_module_path
            return SimpleEventPipeline

        monkeypatch.setattr(gru, "_get_minion_class", resolve_minion_class)
        monkeypatch.setattr(gru, "_get_pipeline_class", resolve_pipeline_class)

        first_start = await gru.start_orchestration(
            pipeline=fake_pipeline_module_path,
            minion=fake_minion_module_path,
            minion_config_path=str(config_path),
        )
        assert first_start.success
        assert first_start.orchestration_id is not None

        first_pipeline = gru._orchestrations[first_start.orchestration_id].pipeline
        assert isinstance(first_pipeline, SimpleEventPipeline)
        await first_pipeline.wait_for_subscribers_then_emit_event()
        await asyncio.wait_for(
            observation_reached.wait(),
            timeout=5.0,
        )
        first_stop = await gru.stop_orchestration(first_start.orchestration_id)
        assert first_stop.success
        assert len(await state_store.get_all_contexts()) == 1

        write_config("beta")
        allow_observation.set()

        second_start = await gru.start_orchestration(
            pipeline=fake_pipeline_module_path,
            minion=fake_minion_module_path,
            minion_config_path=str(config_path),
        )
        assert second_start.success
        assert second_start.orchestration_id == first_start.orchestration_id
        assert second_start.orchestration_id is not None
        assert await logger.wait_for_log(
            "Workflow succeeded",
            timeout=5.0,
            log_kwargs={"orchestration_id": second_start.orchestration_id},
        )
        assert observed_markers == ["alpha", "beta"]
        assert await state_store.get_all_contexts() == []

        resumed_minion = gru._orchestrations[second_start.orchestration_id].minion
        assert isinstance(resumed_minion, ConfigObservingMinion)
        assert resumed_minion.config == MarkerConfig(marker="beta")

        second_stop = await gru.stop_orchestration(second_start.orchestration_id)
        assert second_stop.success
