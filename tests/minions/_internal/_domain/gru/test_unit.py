import asyncio
import types

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._framework.logger import CRITICAL, INFO, WARNING
from minions._internal._framework.metrics_constants import (
    PROCESS_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT,
    SYSTEM_CPU_USED_PERCENT,
    SYSTEM_MEMORY_USED_PERCENT,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics

class TestUnit:
    def patch_sleep_cancel_after(self, monkeypatch, n: int):
        """
        Replace asyncio.sleep with a version that cancels after N calls.
        Lets tests drive multiple loop iterations deterministically.
        """
        calls = {"n": 0}
        async def sleeper(_):
            calls["n"] += 1
            if calls["n"] >= n:
                raise asyncio.CancelledError
        monkeypatch.setattr("asyncio.sleep", sleeper)

    @pytest.mark.asyncio
    async def test_monitor_process_resources_healthy(self, monkeypatch):
        """Single normal iteration: gauges set, counters/histograms untouched."""

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
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_MEMORY_USED_PERCENT], {})["value"] == 12   # 1234/10000*100
        assert InMemoryMetrics.find_sample(gsnap[PROCESS_CPU_USED_PERCENT], {})["value"] == 2       # 16/8

        assert metrics.snapshot_counters() == {}
        assert metrics.snapshot_histograms() == {}

    @pytest.mark.asyncio
    async def test_monitor_high_ram_warn_once_with_reset(self, monkeypatch):
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
    async def test_monitor_failure_suppressed_then_recovery(self, monkeypatch):
        """
        Two failures → one CRITICAL total; next success → one INFO 'recovered'.
        Also assert error payload fields on the CRITICAL log.
        """
        def vm_gen():
            # Fail twice, then return normal forever
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

        self.patch_sleep_cancel_after(monkeypatch, 3)  # two failures, then one success

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



class TestUnitUsingNewAssets:
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


