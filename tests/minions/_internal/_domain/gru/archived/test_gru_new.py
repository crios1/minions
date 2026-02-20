import asyncio
import importlib
import types
from pathlib import Path

import pytest

from minions._internal._domain.gru import Gru

from minions._internal._framework.logger_console import ConsoleLogger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from minions._internal._framework.logger import INFO, WARNING, CRITICAL
from minions._internal._framework.metrics_constants import (
    SYSTEM_MEMORY_USED_PERCENT,
    SYSTEM_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT,
    PROCESS_CPU_USED_PERCENT,
)

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from tests.support.gru_scenario import (
    Directive,
    GruShutdown,
    MinionRunSpec,
    MinionStart,
    MinionStop,
    Concurrent,
    WaitWorkflows,
    run_gru_scenario,
)


TESTS_DIR = Path(__file__).resolve().parents[3]


def reload_emit_n_pipeline(expected_subs: int, total_events: int) -> None:
    mod = importlib.import_module("tests.assets_new.pipeline_emit_n")
    pipeline_cls = getattr(mod, "pipeline", None)
    if pipeline_cls is None:
        raise RuntimeError("tests.assets_new.pipeline_emit_n missing pipeline")
    pipeline_cls.reset_gate(expected_subs=expected_subs, total_events=total_events)


def reload_emit_n_variant(module_name: str, expected_subs: int, total_events: int) -> None:
    mod = importlib.import_module(module_name)
    pipeline_cls = getattr(mod, "pipeline", None)
    if pipeline_cls is None:
        raise RuntimeError(f"{module_name} missing pipeline")
    pipeline_cls.reset_gate(expected_subs=expected_subs, total_events=total_events)


def reload_pipeline_module(module_name: str) -> None:
    mod = importlib.import_module(module_name)
    pipeline_cls = getattr(mod, "pipeline", None)
    if pipeline_cls is None:
        return
    if hasattr(pipeline_cls, "reset_gate"):
        pipeline_cls.reset_gate()
        return
    if hasattr(pipeline_cls, "_emitted"):
        pipeline_cls._emitted = False


class TestUnit:
    def patch_sleep_cancel_after(self, monkeypatch, n: int):
        calls = {"n": 0}
        async def sleeper(_):
            calls["n"] += 1
            if calls["n"] >= n:
                raise asyncio.CancelledError
        monkeypatch.setattr("asyncio.sleep", sleeper)

    @pytest.mark.asyncio
    async def test_monitor_process_resources_healthy(self, monkeypatch):
        monkeypatch.setattr("psutil.virtual_memory", lambda: types.SimpleNamespace(percent=55, total=10_000))
        monkeypatch.setattr("psutil.cpu_percent", lambda interval=None: 20)
        monkeypatch.setattr("psutil.cpu_count", lambda logical=True: 8)

        class Proc:
            def memory_info(self): return types.SimpleNamespace(rss=1234)
            def cpu_percent(self, interval=None): return 16
        monkeypatch.setattr("psutil.Process", lambda: Proc())

        self.patch_sleep_cancel_after(monkeypatch, 1)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        obj = types.SimpleNamespace(_metrics=metrics, _logger=logger)
        obj._monitor_process_resources = types.MethodType(Gru._monitor_process_resources, obj)

        with pytest.raises(asyncio.CancelledError):
            await obj._monitor_process_resources(interval=0)

        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 55
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 20
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 12
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2

        assert metrics.snapshot_counters() == {}
        assert metrics.snapshot_histograms() == {}

    @pytest.mark.asyncio
    async def test_monitor_high_ram_warn_once_with_reset(self, monkeypatch):
        mem_vals = iter([95, 96, 55])
        monkeypatch.setattr(
            "psutil.virtual_memory",
            lambda: types.SimpleNamespace(percent=next(mem_vals), total=10_000),
        )
        monkeypatch.setattr("psutil.cpu_percent", lambda interval=None: 20)
        monkeypatch.setattr("psutil.cpu_count", lambda logical=True: 4)

        class Proc:
            def memory_info(self): return types.SimpleNamespace(rss=1000)
            def cpu_percent(self, interval=None): return 8
        monkeypatch.setattr("psutil.Process", lambda: Proc())

        self.patch_sleep_cancel_after(monkeypatch, 3)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        obj = types.SimpleNamespace(_metrics=metrics, _logger=logger)
        obj._monitor_process_resources = types.MethodType(Gru._monitor_process_resources, obj)

        with pytest.raises(asyncio.CancelledError):
            await obj._monitor_process_resources(interval=0)

        warns = [
            log for log in logger.logs
            if log.level == WARNING and "System memory usage is very high" in log.msg
        ]
        assert len(warns) == 1
        assert warns[0].kwargs.get("system_memory_used_percent") in (95, 96)

        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 55
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 20
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 10
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2

    @pytest.mark.asyncio
    async def test_monitor_failure_suppressed_then_recovery(self, monkeypatch):
        def vm_gen():
            yield from [RuntimeError("boom1"), RuntimeError("boom2")]
            while True:
                yield types.SimpleNamespace(percent=50, total=10_000)

        it = vm_gen()
        def vm_stub():
            v = next(it)
            if isinstance(v, Exception):
                raise v
            return v
        monkeypatch.setattr("psutil.virtual_memory", vm_stub)

        monkeypatch.setattr("psutil.cpu_percent", lambda interval=None: 10)
        monkeypatch.setattr("psutil.cpu_count", lambda logical=True: 4)

        class Proc:
            def memory_info(self): return types.SimpleNamespace(rss=1000)
            def cpu_percent(self, interval=None): return 8
        monkeypatch.setattr("psutil.Process", lambda: Proc())

        self.patch_sleep_cancel_after(monkeypatch, 3)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        obj = types.SimpleNamespace(_metrics=metrics, _logger=logger)
        obj._monitor_process_resources = types.MethodType(Gru._monitor_process_resources, obj)

        with pytest.raises(asyncio.CancelledError):
            await obj._monitor_process_resources(interval=0)

        crits = [log for log in logger.logs if log.level == CRITICAL]
        infos = [log for log in logger.logs if log.level == INFO and "recovered" in log.msg]

        assert len(crits) == 1
        assert len(infos) == 1

        crit = crits[0]
        assert "failed" in crit.msg.lower()
        assert crit.kwargs.get("error_type") in ("RuntimeError",)
        assert crit.kwargs.get("error_message") in ("boom1", "boom2")
        tb = crit.kwargs.get("traceback")
        assert isinstance(tb, str) and "RuntimeError: " in tb

        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 50
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 10
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 10
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2


class TestValidComposition:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_multiple_minions_and_explicit_minion(
            self, gru, logger, metrics, state_store
        ):
            minion_modpath = "tests.assets_new.file_with_two_minions_and_explicit_minion"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

            directives: list[Directive] = [
                MinionStart(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                    expect=MinionRunSpec(),
                ),
                WaitWorkflows(),
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
        async def test_gru_starts_minion_with_multiple_distinct_resource_dependencies(
            self, gru, logger, metrics, state_store
        ):
            minion_modpath = "tests.assets_new.minion_two_steps_multi_resources"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

            directives: list[Directive] = [
                MinionStart(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                ),
                WaitWorkflows(),
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

    class TestPipelineFile:
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_single_pipeline_class(
            self, gru, logger, metrics, state_store
        ):
            minion_modpath = "tests.assets_new.minion_two_steps"
            pipeline_modpath = "tests.assets_new.pipeline_single_class"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_pipeline_module(pipeline_modpath)

            directives: list[Directive] = [
                MinionStart(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                ),
                WaitWorkflows(),
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


class TestValidUsage:
    @pytest.mark.asyncio
    async def test_gru_accepts_none_logger_metrics_state_store(self, gru_factory):
        async with gru_factory(logger=None, state_store=None, metrics=None):
            pass

    @pytest.mark.asyncio
    async def test_gru_allows_create_and_immediate_shutdown(self, gru_factory):
        async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()):
            pass

    @pytest.mark.asyncio
    async def test_gru_start_stop_minion(self, gru, logger, metrics, state_store):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
        reload_emit_n_pipeline(expected_subs=1, total_events=1)

        directives: list[Directive] = [
            MinionStart(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            WaitWorkflows(),
            MinionStop(name_or_instance_id="two-step-minion", expect_success=True),
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
    async def test_gru_start_minion_shutdown_without_stop(
        self, gru, logger, metrics, state_store
    ):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
        reload_emit_n_pipeline(expected_subs=1, total_events=1)

        directives: list[Directive] = [
            MinionStart(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
                expect=MinionRunSpec(),
            ),
            WaitWorkflows(),
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
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(
        self, gru, logger, metrics, state_store
    ):
        minion1 = "tests.assets_new.minion_two_steps_resourced"
        minion2 = "tests.assets_new.minion_two_steps_resourced_b"
        minion3 = "tests.assets_new.minion_two_steps_resourced_c"

        pipeline1 = "tests.assets_new.pipeline_emit_n_a"
        pipeline2 = "tests.assets_new.pipeline_emit_n_b"
        pipeline3 = "tests.assets_new.pipeline_emit_n_c"

        cfg1 = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
        cfg2 = str(TESTS_DIR / "assets_new" / "minion_config_b.toml")
        cfg3 = str(TESTS_DIR / "assets_new" / "minion_config_c.toml")

        reload_emit_n_variant(pipeline1, expected_subs=1, total_events=1)
        reload_emit_n_variant(pipeline2, expected_subs=1, total_events=1)
        reload_emit_n_variant(pipeline3, expected_subs=1, total_events=1)

        directives: list[Directive] = [
            Concurrent(
                MinionStart(minion=minion1, minion_config_path=cfg1, pipeline=pipeline1),
                MinionStart(minion=minion2, minion_config_path=cfg2, pipeline=pipeline2),
                MinionStart(minion=minion3, minion_config_path=cfg3, pipeline=pipeline3),
            ),
            WaitWorkflows(),
            MinionStop(name_or_instance_id="two-step-resourced-minion", expect_success=True),
            MinionStop(name_or_instance_id="two-step-resourced-minion-b", expect_success=True),
            MinionStop(name_or_instance_id="two-step-resourced-minion-c", expect_success=True),
            GruShutdown(expect_success=True),
        ]

        await run_gru_scenario(
            gru,
            logger,
            metrics,
            state_store,
            directives,
            pipeline_event_counts={pipeline1: 1, pipeline2: 1, pipeline3: 1},
            )

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(
        self, gru, logger, metrics, state_store
    ):
        minion_a = "tests.assets_new.minion_two_steps_resourced"
        minion_b = "tests.assets_new.minion_two_steps_resourced_shared_b"
        minion_c = "tests.assets_new.minion_two_steps_resourced_shared_c"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        reload_emit_n_pipeline(expected_subs=3, total_events=1)

        cfg1 = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
        cfg2 = str(TESTS_DIR / "assets_new" / "minion_config_b.toml")
        cfg3 = str(TESTS_DIR / "assets_new" / "minion_config_c.toml")

        directives: list[Directive] = [
            MinionStart(minion=minion_a, minion_config_path=cfg1, pipeline=pipeline_modpath),
            MinionStart(minion=minion_b, minion_config_path=cfg2, pipeline=pipeline_modpath),
            MinionStart(minion=minion_c, minion_config_path=cfg3, pipeline=pipeline_modpath),
            WaitWorkflows(),
            MinionStop(name_or_instance_id="two-step-resourced-minion", expect_success=True),
            MinionStop(name_or_instance_id="two-step-resourced-shared-minion-b", expect_success=True),
            MinionStop(name_or_instance_id="two-step-resourced-shared-minion-c", expect_success=True),
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
    async def test_minion_and_pipeline_share_resource_dependency(
        self, gru, logger, metrics, state_store
    ):
        minion_modpath = "tests.assets_new.minion_two_steps_resourced"
        pipeline_modpath = "tests.assets_new.pipeline_resourced"
        config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
        reload_pipeline_module(pipeline_modpath)

        directives: list[Directive] = [
            MinionStart(minion=minion_modpath, minion_config_path=config_path, pipeline=pipeline_modpath),
            WaitWorkflows(),
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


class TestInvalidComposition:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_minion_file(self, gru_factory):
            minion_modpath = "tests.assets_new.file_empty"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

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
            self, gru_factory
        ):
            minion_modpath = "tests.assets_new.file_with_two_minions"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

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
            self, gru_factory
        ):
            minion_modpath = "tests.assets_new.file_with_invalid_explicit_minion"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

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
            self, gru_factory
        ):
            minion_modpath = "tests.assets_new.minion_bad_context"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

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
            self, gru_factory
        ):
            minion_modpath = "tests.assets_new.minion_bad_event"
            pipeline_modpath = "tests.assets_new.pipeline_emit_n"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
            reload_emit_n_pipeline(expected_subs=1, total_events=1)

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
        async def test_gru_returns_error_on_empty_pipeline_file(self, gru_factory):
            minion_modpath = "tests.assets_new.minion_two_steps"
            pipeline_modpath = "tests.assets_new.file_empty"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")

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
        async def test_gru_returns_error_on_pipeline_file_with_multiple_pipelines_and_no_explicit_pipeline(self, gru_factory):
            minion_modpath = "tests.assets_new.minion_two_steps"
            pipeline_modpath = "tests.assets_new.file_with_two_pipelines"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")

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
        async def test_gru_returns_error_on_pipeline_file_with_invalid_explicit_pipeline(self, gru_factory):
            minion_modpath = "tests.assets_new.minion_two_steps"
            pipeline_modpath = "tests.assets_new.file_with_invalid_explicit_pipeline"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")

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
        async def test_gru_returns_error_on_pipeline_event_not_serializable(self, gru_factory):
            minion_modpath = "tests.assets_new.minion_two_steps"
            pipeline_modpath = "tests.assets_new.pipeline_unserializable_event"
            config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")

            async with gru_factory(state_store=NoOpStateStore(), logger=NoOpLogger(), metrics=NoOpMetrics()) as gru:
                result = await gru.start_minion(
                    minion=minion_modpath,
                    minion_config_path=config_path,
                    pipeline=pipeline_modpath,
                )

                assert not result.success
                assert result.reason
                assert "event type is not JSON-serializable" in result.reason


class TestInvalidUsage:
    @pytest.mark.asyncio
    async def test_gru_raises_on_direct_instantiation(self):
        with pytest.raises(RuntimeError):
            Gru(
                loop=asyncio.get_running_loop(),
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics(),
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_multiple_instances(self, gru_factory):
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
    async def test_gru_raises_on_invalid_logger_param(self, bad_logger):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=bad_logger,
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore(),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics", [123, "invalid"])
    async def test_gru_raises_on_invalid_metrics_param(self, bad_metrics):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=bad_metrics,
                state_store=NoOpStateStore(),
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_state_store", [123, "invalid"])
    async def test_gru_raises_on_invalid_state_store_param(self, bad_state_store):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=bad_state_store,
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_minion(self, gru_factory):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_emit_n"
        config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")
        reload_emit_n_pipeline(expected_subs=1, total_events=1)

        async with gru_factory(state_store=NoOpStateStore(), logger=ConsoleLogger(), metrics=NoOpMetrics()) as gru:
            result1 = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
            )

            assert result1.success
            assert result1.name == "two-step-minion"
            assert result1.instance_id in gru._minions_by_id
            assert result1.instance_id in gru._minion_tasks

            result2 = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
            )

            assert not result2.success
            assert result2.reason
            assert "Minion already running" in result2.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_minion(self, gru_factory):
        async with gru_factory(state_store=NoOpStateStore(), logger=ConsoleLogger(), metrics=NoOpMetrics()) as gru:
            result = await gru.stop_minion("mock")

            assert not result.success
            assert result.reason
            assert "No minion found" in result.reason

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(self, gru_factory):
        minion_modpath = "tests.assets_new.minion_two_steps"
        pipeline_modpath = "tests.assets_new.pipeline_dict_event"
        config_path = str(TESTS_DIR / "assets_new" / "minion_config_a.toml")

        async with gru_factory(state_store=NoOpStateStore(), logger=ConsoleLogger(), metrics=NoOpMetrics()) as gru:
            result = await gru.start_minion(
                minion=minion_modpath,
                minion_config_path=config_path,
                pipeline=pipeline_modpath,
            )

            assert not result.success
            assert result.reason
            assert "Incompatible minion and pipeline event types" in result.reason
