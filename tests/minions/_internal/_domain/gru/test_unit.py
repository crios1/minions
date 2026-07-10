import asyncio
import contextlib
import hashlib
import json
import types
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

from minions import Minion, Pipeline, Resource, minion_id, minion_step, pipeline_id, resource_id
from minions._internal._domain.component_identity import get_component_id
from minions._internal._domain.gru import ORCHESTRATION_ID_VERSION, Gru
from minions._internal._domain.gru_result_types import StartResult, StopResult
from minions._internal._framework.async_component import AsyncComponent
from minions._internal._framework.logger import CRITICAL, INFO, WARNING
from minions._internal._framework.metrics_constants import (
    PROCESS_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT,
    SYSTEM_CPU_USED_PERCENT,
    SYSTEM_MEMORY_USED_PERCENT,
)
from minions._internal._framework.state_store_noop import NoOpStateStore
from minions._internal._utils.base62_encode import base62_encode
from tests.assets.contexts.simple import SimpleContext
from tests.assets.events.simple import SimpleEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.minions._internal._domain.gru.assertions import assert_runtime_empty


class _ResourceMonitorHarness:
    _monitor_process_resources = Gru._monitor_process_resources

    def __init__(self, metrics: InMemoryMetrics, logger: InMemoryLogger) -> None:
        self._metrics = metrics
        self._logger = logger


def _cpu_percent_stub(value: int) -> Callable[[object | None], int]:
    def _cpu_percent(interval: object | None = None) -> int:
        return value
    return _cpu_percent


def _cpu_count_stub(value: int) -> Callable[[bool], int]:
    def _cpu_count(logical: bool = True) -> int:
        return value
    return _cpu_count


def _process_stub(*, rss: int, cpu_percent: int) -> Callable[[], object]:
    class Proc:
        def memory_info(self) -> types.SimpleNamespace:
            return types.SimpleNamespace(rss=rss)
        def cpu_percent(self, interval: object | None = None) -> int:
            return cpu_percent
    return Proc


@dataclass
class InlineIdentityConfig:
    name: str


MINION_COMPONENT_ID = "11111111-1111-4111-8111-11111111111a"
PIPELINE_COMPONENT_ID = "22222222-2222-4222-8222-22222222222b"
RESOURCE_COMPONENT_ID = "33333333-3333-4333-8333-33333333333c"
CONFIG_ID = "44444444-4444-4444-8444-44444444444f"


class TestUnit:
    def _expected_orchestration_id(
        self,
        *,
        minion_id: str,
        minion_config_id: str,
        pipeline_id: str,
    ) -> str:
        payload = {
            "version": ORCHESTRATION_ID_VERSION,
            "minion_id": minion_id,
            "minion_config_id": minion_config_id,
            "pipeline_id": pipeline_id,
        }
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
        # Base62 makes the SHA-256-backed ID prettier: alphanumeric and shorter than hex.
        # Left-padding keeps the ID consistently 44 characters long.
        return base62_encode(hashlib.sha256(serialized).digest()).rjust(44, "0")

    def test_attached_component_ids_are_stable_identities(self) -> None:
        @minion_id(MINION_COMPONENT_ID)
        class StableIdMinion(Minion[SimpleEvent, SimpleContext]):
            @minion_step
            async def step(self) -> None:
                return None

        @pipeline_id(PIPELINE_COMPONENT_ID)
        class StableIdPipeline(Pipeline[SimpleEvent]):
            async def produce_event(self) -> SimpleEvent:
                return SimpleEvent(timestamp=0)

        @resource_id(RESOURCE_COMPONENT_ID)
        class StableIdResource(Resource):
            pass

        assert Gru._get_component_identity(StableIdMinion, "fallback.minion") == MINION_COMPONENT_ID
        assert (
            Gru._get_component_identity(StableIdPipeline, "fallback.pipeline")
            == PIPELINE_COMPONENT_ID
        )
        assert (
            Gru._get_component_identity(StableIdResource, "fallback.resource")
            == RESOURCE_COMPONENT_ID
        )
        assert Gru._get_minion_identity(StableIdMinion) == MINION_COMPONENT_ID

    def test_idless_components_keep_fallback_identity(self) -> None:
        class PrototypeResource(Resource):
            pass

        assert (
            Gru._get_component_identity(PrototypeResource, "fallback.prototype")
            == "fallback.prototype"
        )

    def test_minion_string_entrypoint_identity_fallback_preserves_entrypoint_module_path(
        self,
    ) -> None:
        from tests.assets.minions.two_steps.simple.default import (
            AssetMinion as TwoStepSimpleMinion,
        )

        gru = object.__new__(Gru)
        entrypoint_module_path = "tests.assets.entrypoints.valid.reexported_minion_subclass"

        assert Gru._get_minion_identity(TwoStepSimpleMinion) == (
            "tests.assets.minions.two_steps.simple.default.AssetMinion"
        )
        assert gru._get_minion_identity_from_module_path(entrypoint_module_path) == (
            entrypoint_module_path
        )

    def test_pipeline_string_entrypoint_identity_fallback_preserves_entrypoint_module_path(
        self,
    ) -> None:
        from tests.assets.pipelines.emit_one.simple.default import (
            AssetPipeline as EmitOneSimplePipeline,
        )

        gru = object.__new__(Gru)
        entrypoint_module_path = "tests.assets.entrypoints.valid.reexported_pipeline_subclass"

        assert gru._get_pipeline_identity(EmitOneSimplePipeline) == (
            "tests.assets.pipelines.emit_one.simple.default.AssetPipeline"
        )
        assert gru._get_pipeline_identity_from_module_path(entrypoint_module_path) == (
            entrypoint_module_path
        )

    def test_attached_component_identity_ignores_current_address(self) -> None:
        @resource_id(RESOURCE_COMPONENT_ID)
        class AddressStableResource(Resource):
            pass

        original_module = AddressStableResource.__module__
        try:
            AddressStableResource.__module__ = "moved.module"
            assert (
                Gru._get_component_identity(
                    AddressStableResource, "moved.module.AddressStableResource"
                )
                == RESOURCE_COMPONENT_ID
            )
        finally:
            AddressStableResource.__module__ = original_module

    def test_inline_config_identity_is_stable_hashed_and_content_sensitive(self) -> None:
        cfg_a = Gru._make_inline_config_identity(InlineIdentityConfig(name="alpha"))
        cfg_a_again = Gru._make_inline_config_identity(InlineIdentityConfig(name="alpha"))
        cfg_b = Gru._make_inline_config_identity(InlineIdentityConfig(name="bravo"))

        assert cfg_a == cfg_a_again
        assert cfg_a != cfg_b
        assert cfg_a.startswith("<inline:")
        assert "alpha" not in cfg_a

        key = Gru._make_orchestration_id(
            pipeline_id="tests.assets.Pipeline",
            minion_id="tests.assets.Minion",
            minion_config_id=cfg_a,
        )
        assert key == self._expected_orchestration_id(
            minion_id="tests.assets.Minion",
            minion_config_id=cfg_a,
            pipeline_id="tests.assets.Pipeline",
        )
        assert len(key) == 44

    def test_config_id_is_used_for_toml_config_identity(self, tmp_path: Path) -> None:
        config_path = tmp_path / "renamable.toml"
        config_path.write_text(
            f'_minions_config_id = "{CONFIG_ID}"\n\n[config]\nname = "alpha"\n'
        )

        assert Gru._get_config_identity(str(config_path)) == CONFIG_ID
        assert Gru._make_orchestration_id(
            pipeline_id="pipeline-id",
            minion_id="minion-id",
            minion_config_id=CONFIG_ID,
        ) == self._expected_orchestration_id(
            minion_id="minion-id",
            minion_config_id=CONFIG_ID,
            pipeline_id="pipeline-id",
        )

    def test_config_id_is_used_for_json_config_identity(self, tmp_path: Path) -> None:
        config_path = tmp_path / "renamable.json"
        config_path.write_text(f'{{"_minions_config_id": "{CONFIG_ID}", "name": "alpha"}}')

        assert Gru._get_config_identity(str(config_path)) == CONFIG_ID

    def test_config_id_is_used_for_yaml_config_identity(self, tmp_path: Path) -> None:
        config_path = tmp_path / "renamable.yaml"
        config_path.write_text(f'_minions_config_id: "{CONFIG_ID}"\nname: alpha\n')

        assert Gru._get_config_identity(str(config_path)) == CONFIG_ID

    def test_idless_config_keeps_path_fallback_identity(self, tmp_path: Path) -> None:
        config_path = tmp_path / "fallback.toml"
        config_path.write_text('[config]\nname = "alpha"\n')

        assert Gru._get_config_identity(str(config_path)) == config_path.resolve().as_posix()

    def test_config_id_must_be_canonical_uuid(self, tmp_path: Path) -> None:
        config_path = tmp_path / "bad.toml"
        config_path.write_text('_minions_config_id = "not-a-uuid"\n')

        with pytest.raises(ValueError, match="config id"):
            Gru._get_config_identity(str(config_path))

    @pytest.mark.asyncio
    async def test_start_and_stop_delegate_to_canonical_methods(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        gru = object.__new__(Gru)
        start_calls: list[tuple[object, object, object | None, object | None]] = []
        stop_calls: list[str] = []

        async def fake_start_orchestration(
            self: Gru,
            pipeline: object,
            minion: object,
            *,
            minion_config: object | None = None,
            minion_config_path: str | None = None,
        ) -> StartResult:
            start_calls.append((pipeline, minion, minion_config, minion_config_path))
            return StartResult(success=True, orchestration_id="iid")

        async def fake_stop_orchestration(self: Gru, id: str) -> StopResult:
            stop_calls.append(id)
            return StopResult(success=True)

        monkeypatch.setattr(Gru, "start_orchestration", fake_start_orchestration)
        monkeypatch.setattr(Gru, "stop_orchestration", fake_stop_orchestration)

        start_result = await gru.start("pipeline", "minion", minion_config_path="cfg")
        stop_result = await gru.stop("iid")

        assert start_result.success
        assert stop_result.success
        assert start_calls == [("pipeline", "minion", None, "cfg")]
        assert stop_calls == ["iid"]

    def patch_sleep_cancel_after(self, monkeypatch: pytest.MonkeyPatch, n: int) -> None:
        """
        Replace asyncio.sleep with a version that cancels after N calls.
        Lets tests drive multiple loop iterations deterministically.
        """
        calls = {"n": 0}

        async def sleeper(_delay: object) -> None:
            calls["n"] += 1
            if calls["n"] >= n:
                raise asyncio.CancelledError

        monkeypatch.setattr("asyncio.sleep", sleeper)

    @pytest.mark.asyncio
    async def test_monitor_process_resources_healthy(
        self,
        monkeypatch: pytest.MonkeyPatch,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        """Single normal iteration: gauges set, counters/histograms untouched."""

        monkeypatch.setattr(
            "psutil.virtual_memory", lambda: types.SimpleNamespace(percent=55, total=10_000)
        )
        monkeypatch.setattr("psutil.cpu_percent", _cpu_percent_stub(20))
        monkeypatch.setattr("psutil.cpu_count", _cpu_count_stub(8))
        monkeypatch.setattr("psutil.Process", _process_stub(rss=1234, cpu_percent=16))

        self.patch_sleep_cancel_after(monkeypatch, 1)

        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

        assert metrics.snapshot_gauge_value(SYSTEM_MEMORY_USED_PERCENT, {}) == 55
        assert metrics.snapshot_gauge_value(SYSTEM_CPU_USED_PERCENT, {}) == 20
        assert metrics.snapshot_gauge_value(PROCESS_MEMORY_USED_PERCENT, {}) == 12
        assert metrics.snapshot_gauge_value(PROCESS_CPU_USED_PERCENT, {}) == 2

        assert metrics.snapshot_counters() == {}
        assert metrics.snapshot_histograms() == {}

    @pytest.mark.asyncio
    async def test_monitor_high_ram_warn_once_with_reset(
        self,
        monkeypatch: pytest.MonkeyPatch,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        """
        High RAM across two iterations should log exactly one WARNING,
        then drop to normal without logging another warning; also assert kwargs.
        """
        # sys_mem: high -> high -> normal
        mem_vals = iter([95, 96, 55])
        monkeypatch.setattr(
            "psutil.virtual_memory",
            lambda: types.SimpleNamespace(percent=next(mem_vals), total=10_000),
        )
        monkeypatch.setattr("psutil.cpu_percent", _cpu_percent_stub(20))
        monkeypatch.setattr("psutil.cpu_count", _cpu_count_stub(4))
        monkeypatch.setattr("psutil.Process", _process_stub(rss=1000, cpu_percent=8))

        self.patch_sleep_cancel_after(monkeypatch, 3)

        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

        # Exactly one warning across two high-usage iterations
        warns = [
            log
            for log in logger.logs
            if log.level == WARNING and "System memory usage is very high" in log.msg
        ]
        assert len(warns) == 1
        assert warns[0].kwargs.get("system_memory_used_percent") in (95, 96)

        # Metrics reflect the last (normal) iteration too
        assert metrics.snapshot_gauge_value(SYSTEM_MEMORY_USED_PERCENT, {}) == 55
        assert metrics.snapshot_gauge_value(SYSTEM_CPU_USED_PERCENT, {}) == 20
        assert metrics.snapshot_gauge_value(PROCESS_MEMORY_USED_PERCENT, {}) == 10
        assert metrics.snapshot_gauge_value(PROCESS_CPU_USED_PERCENT, {}) == 2

    @pytest.mark.asyncio
    async def test_monitor_failure_suppressed_then_recovery(
        self,
        monkeypatch: pytest.MonkeyPatch,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        """
        Two failures → one CRITICAL total; next success → one INFO 'recovered'.
        Also assert error payload fields on the CRITICAL log.
        """

        def vm_gen() -> Iterator[RuntimeError | types.SimpleNamespace]:
            # Fail twice, then return normal forever
            yield from [RuntimeError("boom1"), RuntimeError("boom2")]
            while True:
                yield types.SimpleNamespace(percent=50, total=10_000)

        it = vm_gen()

        def vm_stub() -> types.SimpleNamespace:
            v = next(it)
            if isinstance(v, Exception):
                raise v
            return v

        monkeypatch.setattr("psutil.virtual_memory", vm_stub)

        monkeypatch.setattr("psutil.cpu_percent", _cpu_percent_stub(10))
        monkeypatch.setattr("psutil.cpu_count", _cpu_count_stub(4))
        monkeypatch.setattr("psutil.Process", _process_stub(rss=1000, cpu_percent=8))

        self.patch_sleep_cancel_after(monkeypatch, 3)  # two failures, then one success

        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

        crits = [log for log in logger.logs if log.level == CRITICAL]
        infos = [log for log in logger.logs if log.level == INFO and "recovered" in log.msg]

        assert len(crits) == 1
        assert len(infos) == 1

        # Structured error payload from the CRITICAL
        crit = crits[0]
        assert "failed" in crit.msg.lower()
        assert crit.kwargs.get("error_type") in ("RuntimeError",)
        assert crit.kwargs.get("error_message") in ("boom1", "boom2")
        tb = crit.kwargs.get("traceback")
        assert isinstance(tb, str) and "RuntimeError: " in tb

        # After recovery, gauges should be present (from the success iteration)
        assert metrics.snapshot_gauge_value(SYSTEM_MEMORY_USED_PERCENT, {}) == 50
        assert metrics.snapshot_gauge_value(SYSTEM_CPU_USED_PERCENT, {}) == 10
        assert metrics.snapshot_gauge_value(PROCESS_MEMORY_USED_PERCENT, {}) == 10
        assert metrics.snapshot_gauge_value(PROCESS_CPU_USED_PERCENT, {}) == 2

    @pytest.mark.asyncio
    async def test_shutdown_surfaces_internal_shutdown_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        async with managed_gru_context(
            logger=logger,
            metrics=metrics,
            state_store=NoOpStateStore(),
        ) as gru:

            async def failing_shutdown_async_component(
                _comp: AsyncComponent, log_kwargs: dict[str, object] | None = None
            ) -> None:
                raise RuntimeError("component shutdown boom")

            monkeypatch.setattr(gru, "_shutdown_async_component", failing_shutdown_async_component)
            result = await gru.shutdown()

            assert not result.success
            assert result.reason is not None
            assert "internal error" in result.reason
            assert len(result.errors) == 2
            assert all(e.phase == "shutdown_component" for e in result.errors)
            assert {e.component for e in result.errors} == {"state_store", "metrics"}
            assert all(e.error_type == "RuntimeError" for e in result.errors)
            assert all("component shutdown boom" in e.error_message for e in result.errors)

    @pytest.mark.asyncio
    async def test_shutdown_clears_runtime_state_when_component_shutdown_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        async with managed_gru_context(
            logger=logger,
            metrics=metrics,
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_orchestration(
                "tests.assets.crash.pipelines.counter.healthy",
                "tests.assets.crash.minions.counter.healthy",
            )
            assert result.success

            async def failing_shutdown_async_component(
                _comp: AsyncComponent, log_kwargs: dict[str, object] | None = None
            ) -> None:
                raise RuntimeError("component shutdown boom")

            monkeypatch.setattr(gru, "_shutdown_async_component", failing_shutdown_async_component)
            shutdown = await gru.shutdown()

            assert not shutdown.success
            assert_runtime_empty(gru)

    @pytest.mark.asyncio
    async def test_runtime_state_snapshot_is_exact_detached_and_immutable(
        self,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        from tests.assets.minions.two_steps.counter.identified_with_fixed_resource import (
            AssetMinion as IdentifiedFixedResourceMinion,
        )
        from tests.assets.pipelines.emit_one.counter.identified import (
            AssetPipeline as IdentifiedEmitOneCounterPipeline,
        )
        from tests.assets.resources.fixed.identified import (
            AssetResource as IdentifiedFixedResource,
        )

        identified_pipeline_id = get_component_id(IdentifiedEmitOneCounterPipeline)
        identified_resource_id = get_component_id(IdentifiedFixedResource)
        assert identified_pipeline_id is not None
        assert identified_resource_id is not None

        async with managed_gru_context(
            logger=logger,
            metrics=metrics,
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_orchestration(
                IdentifiedEmitOneCounterPipeline,
                IdentifiedFixedResourceMinion,
            )
            assert result.success
            assert result.orchestration_id is not None
            minion = gru._minions_by_orchestration_id[result.orchestration_id]
            minion_instance_id = minion._mn_minion_instance_id

            snapshot = gru.runtime_state_snapshot()
            assert snapshot.minion_instances == {minion_instance_id}
            assert snapshot.orchestrations == {result.orchestration_id}
            assert snapshot.minion_tasks == {minion_instance_id}
            assert snapshot.pipelines == {identified_pipeline_id}
            assert snapshot.pipeline_tasks == {identified_pipeline_id}
            assert snapshot.resources == {identified_resource_id}
            assert snapshot.resource_tasks == {identified_resource_id}
            assert snapshot.minion_instance_by_orchestration == {
                result.orchestration_id: minion_instance_id
            }
            assert snapshot.pipeline_by_minion_instance == {
                minion_instance_id: identified_pipeline_id
            }
            assert snapshot.resources_by_minion_instance == {
                minion_instance_id: frozenset({identified_resource_id})
            }
            assert snapshot.resources_by_pipeline == {}
            assert snapshot.resource_dependencies_by_dependent_resource == {}
            assert snapshot.resource_dependents_by_dependency_resource == {}
            assert snapshot.resource_reference_counts == {identified_resource_id: 1}
            assert snapshot.minion_instance_for_orchestration(
                result.orchestration_id
            ) == minion_instance_id
            assert snapshot.pipeline_for_minion(minion_instance_id) == identified_pipeline_id
            assert snapshot.resources_for_minion(minion_instance_id) == frozenset(
                {identified_resource_id}
            )
            assert snapshot.resources_for_pipeline(identified_pipeline_id) == frozenset()
            assert snapshot.dependencies_for_resource(identified_resource_id) == frozenset()
            assert snapshot.dependents_for_resource(identified_resource_id) == frozenset()
            assert snapshot.resource_refcount(identified_resource_id) == 1
            assert snapshot.resource_refcount("missing") == 0

            with pytest.raises(TypeError):
                snapshot.pipeline_by_minion_instance["other"] = "pipeline"  # type: ignore[index]

            # Mutating Gru after capture must not change the point-in-time snapshot.
            snapshot_pipeline_map = dict(snapshot.pipeline_by_minion_instance)
            gru._minion_pipeline_map["other"] = "pipeline"
            try:
                assert snapshot.pipeline_by_minion_instance == snapshot_pipeline_map
            finally:
                gru._minion_pipeline_map.pop("other")

    @pytest.mark.asyncio
    async def test_shutdown_reports_task_cancel_errors_and_clears_runtime_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        async with managed_gru_context(
            logger=logger,
            metrics=metrics,
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_orchestration(
                "tests.assets.crash.pipelines.counter.healthy",
                "tests.assets.crash.minions.counter.healthy",
            )
            assert result.success

            async def failing_safe_cancel_task(*args: object, **kwargs: object):
                raise RuntimeError("cancel boom")

            monkeypatch.setattr(
                "minions._internal._domain.gru.safe_cancel_task",
                failing_safe_cancel_task,
            )
            shutdown = await gru.shutdown()

            assert not shutdown.success
            assert shutdown.reason == "Gru shutdown completed with 3 internal error(s)."
            assert len(shutdown.errors) == 3
            assert all(error.phase == "cancel_task" for error in shutdown.errors)
            assert all(error.error_message == "cancel boom" for error in shutdown.errors)
            assert_runtime_empty(gru)

    @pytest.mark.asyncio
    async def test_shutdown_clears_runtime_state_when_initial_log_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        async with managed_gru_context(
            logger=logger,
            metrics=metrics,
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_orchestration(
                "tests.assets.crash.pipelines.counter.healthy",
                "tests.assets.crash.minions.counter.healthy",
            )
            assert result.success
            monitor_task = gru._resource_monitor_task
            shutdown_components: list[AsyncComponent] = []

            async def tracking_shutdown_async_component(
                comp: AsyncComponent, log_kwargs: dict[str, object] | None = None
            ) -> None:
                shutdown_components.append(comp)

            async def failing_log(*args: object, **kwargs: object) -> None:
                raise RuntimeError("log boom")

            monkeypatch.setattr(gru, "_shutdown_async_component", tracking_shutdown_async_component)
            monkeypatch.setattr(gru._logger, "log", failing_log)
            shutdown = await gru.shutdown()

            assert shutdown.success
            assert shutdown_components == [gru._state_store, gru._metrics]
            assert monitor_task.done()
            assert monitor_task.cancelled()
            assert_runtime_empty(gru)

    @pytest.mark.asyncio
    async def test_shutdown_clears_runtime_state_when_logger_shutdown_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        managed_gru_context: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
    ):
        async with managed_gru_context(
            logger=logger,
            metrics=metrics,
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_orchestration(
                "tests.assets.crash.pipelines.counter.healthy",
                "tests.assets.crash.minions.counter.healthy",
            )
            assert result.success

            async def failing_logger_shutdown() -> None:
                raise RuntimeError("logger shutdown boom")

            monkeypatch.setattr(gru._logger, "_mn_shutdown", failing_logger_shutdown)
            shutdown = await gru.shutdown()

            assert not shutdown.success
            assert shutdown.reason == "logger shutdown boom"
            assert_runtime_empty(gru)
