import asyncio
import contextlib
import types
from collections.abc import Callable, Iterator

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._framework.async_component import AsyncComponent
from minions._internal._framework.logger import CRITICAL, INFO, WARNING
from minions._internal._framework.metrics_constants import (
    PROCESS_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT,
    SYSTEM_CPU_USED_PERCENT,
    SYSTEM_MEMORY_USED_PERCENT,
)
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics


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


class TestUnit:
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
    async def test_monitor_process_resources_healthy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """Single normal iteration: gauges set, counters/histograms untouched."""

        monkeypatch.setattr("psutil.virtual_memory", lambda: types.SimpleNamespace(percent=55, total=10_000))
        monkeypatch.setattr("psutil.cpu_percent", _cpu_percent_stub(20))
        monkeypatch.setattr("psutil.cpu_count", _cpu_count_stub(8))
        monkeypatch.setattr("psutil.Process", _process_stub(rss=1234, cpu_percent=16))

        self.patch_sleep_cancel_after(monkeypatch, 1)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 55
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 20
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 12   # 1234/10000*100
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2       # 16/8

        assert metrics.snapshot_counters() == {}
        assert metrics.snapshot_histograms() == {}

    @pytest.mark.asyncio
    async def test_monitor_high_ram_warn_once_with_reset(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

        # Exactly one warning across two high-usage iterations
        warns = [
            log for log in logger.logs
            if log.level == WARNING
            and "System memory usage is very high" in log.msg
        ]
        assert len(warns) == 1
        assert warns[0].kwargs.get("system_memory_used_percent") in (95, 96)

        # Metrics reflect the last (normal) iteration too
        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 55
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 20
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 10  # 1000/10000*100
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2      # int(8/4)

    @pytest.mark.asyncio
    async def test_monitor_failure_suppressed_then_recovery(self, monkeypatch: pytest.MonkeyPatch) -> None:
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

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
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
        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 50
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 10
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 10
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2

    @pytest.mark.asyncio
    async def test_shutdown_surfaces_internal_shutdown_errors(
        self,
        monkeypatch: pytest.MonkeyPatch,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            logger=InMemoryLogger(),
            metrics=InMemoryMetrics(),
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
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            logger=InMemoryLogger(),
            metrics=InMemoryMetrics(),
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_minion(
                "tests.assets.crash.minions.good",
                "tests.assets.crash.pipelines.emit_1_then_block",
            )
            assert result.success

            async def failing_shutdown_async_component(
                _comp: AsyncComponent, log_kwargs: dict[str, object] | None = None
            ) -> None:
                raise RuntimeError("component shutdown boom")

            monkeypatch.setattr(gru, "_shutdown_async_component", failing_shutdown_async_component)
            shutdown = await gru.shutdown()

            assert not shutdown.success
            assert gru._runtime_state_snapshot() == {}

    @pytest.mark.asyncio
    async def test_shutdown_reports_task_cancel_errors_and_clears_runtime_state(
        self,
        monkeypatch: pytest.MonkeyPatch,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            logger=InMemoryLogger(),
            metrics=InMemoryMetrics(),
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_minion(
                "tests.assets.crash.minions.good",
                "tests.assets.crash.pipelines.emit_1_then_block",
            )
            assert result.success

            async def failing_safe_cancel_task(
                *args: object, **kwargs: object
            ) -> None:
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
            assert gru._runtime_state_snapshot() == {}

    @pytest.mark.asyncio
    async def test_shutdown_clears_runtime_state_when_initial_log_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            logger=InMemoryLogger(),
            metrics=InMemoryMetrics(),
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_minion(
                "tests.assets.crash.minions.good",
                "tests.assets.crash.pipelines.emit_1_then_block",
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
            assert gru._runtime_state_snapshot() == {}

    @pytest.mark.asyncio
    async def test_shutdown_clears_runtime_state_when_logger_shutdown_fails(
        self,
        monkeypatch: pytest.MonkeyPatch,
        gru_factory: Callable[..., contextlib.AbstractAsyncContextManager[Gru]],
    ) -> None:
        async with gru_factory(
            logger=InMemoryLogger(),
            metrics=InMemoryMetrics(),
            state_store=NoOpStateStore(),
        ) as gru:
            result = await gru.start_minion(
                "tests.assets.crash.minions.good",
                "tests.assets.crash.pipelines.emit_1_then_block",
            )
            assert result.success

            async def failing_logger_shutdown() -> None:
                raise RuntimeError("logger shutdown boom")

            monkeypatch.setattr(gru._logger, "_mn_shutdown", failing_logger_shutdown)
            shutdown = await gru.shutdown()

            assert not shutdown.success
            assert shutdown.reason == "logger shutdown boom"
            assert gru._runtime_state_snapshot() == {}



class TestUnitUsingNewAssets:
    def patch_sleep_cancel_after(self, monkeypatch: pytest.MonkeyPatch, n: int) -> None:
        calls = {"n": 0}

        async def sleeper(_delay: object) -> None:
            calls["n"] += 1
            if calls["n"] >= n:
                raise asyncio.CancelledError
        monkeypatch.setattr("asyncio.sleep", sleeper)

    @pytest.mark.asyncio
    async def test_monitor_process_resources_healthy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr("psutil.virtual_memory", lambda: types.SimpleNamespace(percent=55, total=10_000))
        monkeypatch.setattr("psutil.cpu_percent", _cpu_percent_stub(20))
        monkeypatch.setattr("psutil.cpu_count", _cpu_count_stub(8))
        monkeypatch.setattr("psutil.Process", _process_stub(rss=1234, cpu_percent=16))

        self.patch_sleep_cancel_after(monkeypatch, 1)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

        gsnap = metrics.snapshot_gauges()
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_MEMORY_USED_PERCENT], {})["value"] == 55
        assert InMemoryMetrics.find_sample(gsnap[SYSTEM_CPU_USED_PERCENT], {})["value"] == 20
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 12
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2

        assert metrics.snapshot_counters() == {}
        assert metrics.snapshot_histograms() == {}

    @pytest.mark.asyncio
    async def test_monitor_high_ram_warn_once_with_reset(self, monkeypatch: pytest.MonkeyPatch) -> None:
        mem_vals = iter([95, 96, 55])
        monkeypatch.setattr(
            "psutil.virtual_memory",
            lambda: types.SimpleNamespace(percent=next(mem_vals), total=10_000),
        )
        monkeypatch.setattr("psutil.cpu_percent", _cpu_percent_stub(20))
        monkeypatch.setattr("psutil.cpu_count", _cpu_count_stub(4))
        monkeypatch.setattr("psutil.Process", _process_stub(rss=1000, cpu_percent=8))

        self.patch_sleep_cancel_after(monkeypatch, 3)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

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
    async def test_monitor_failure_suppressed_then_recovery(self, monkeypatch: pytest.MonkeyPatch) -> None:
        def vm_gen() -> Iterator[RuntimeError | types.SimpleNamespace]:
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

        self.patch_sleep_cancel_after(monkeypatch, 3)

        metrics = InMemoryMetrics()
        logger = InMemoryLogger()
        monitor = _ResourceMonitorHarness(metrics, logger)

        with pytest.raises(asyncio.CancelledError):
            await monitor._monitor_process_resources(interval=0)

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
