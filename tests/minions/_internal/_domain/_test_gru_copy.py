import asyncio
import types
import pytest
import pytest_asyncio

from pathlib import Path
from dataclasses import dataclass, field
from typing import Any, Iterable

import minions._internal._domain.gru as grumod

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion
from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.gru_result_types import StartMinionResult

from tests.assets.support.mixin_spy import SpyMixin

# TODO: replace below NoOp(s) with below InMemory(s) w/ robust checks
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore

from minions._internal._framework.logger_console import ConsoleLogger
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from minions._internal._framework.logger import DEBUG, INFO, WARNING, ERROR, CRITICAL
from minions._internal._framework.metrics_constants import (
    SYSTEM_MEMORY_USED_PERCENT, SYSTEM_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT, PROCESS_CPU_USED_PERCENT
)

TESTS_DIR = Path(__file__).resolve().parents[3]

# @pytest.fixture(autouse=True)
# def reset_gru_singleton():
#     grumod._GRU_SINGLETON = None
#     yield
#     grumod._GRU_SINGLETON = None


@pytest_asyncio.fixture(autouse=True)
async def reset_test_env():
    # ensure singleton cleared at test start and spying enabled for helpers
    grumod._GRU_SINGLETON = None
    InMemoryLogger.enable_spy()
    InMemoryMetrics.enable_spy()
    InMemoryStateStore.enable_spy()

    # Reload non-support test asset modules so module-level state (like
    # pipeline.total_events) is reset between tests. We intentionally skip
    # modules under `tests.assets.support` to avoid replacing the canonical
    # SpyMixin and helper classes used by tests.
    # TODO: i might move this logic into the helperized version of my gru tests
    #       then that assets are reloaded before they are reused
    import importlib, sys
    for mname in list(sys.modules.keys()):
        if mname.startswith("tests.assets.") and not mname.startswith("tests.assets.support"):
            try:
                importlib.reload(sys.modules[mname])
            except Exception:
                # best-effort reload; ignore failures during reload
                pass

    def _cls_snap(cls):
        # safe snapshot of SpyMixin-like registries
        return {
            "counts": dict(getattr(cls, "_mspy_counts", {})) if getattr(cls, "_mspy_counts", None) is not None else None,
            "wrapped_ids": set(getattr(cls, "_mspy_wrapped_ids", set())) if getattr(cls, "_mspy_wrapped_ids", None) is not None else None,
            "wrapped_fns_count": len(getattr(cls, "_mspy_wrapped_fns", ())) if getattr(cls, "_mspy_wrapped_fns", None) is not None else None,
            "waiters_keys": list(getattr(cls, "_mspy_waiters", {}).keys()) if getattr(cls, "_mspy_waiters", None) is not None else None,
        }

    def _snap():
        try:
            spy = SpyMixin
        except Exception:
            from tests.assets.support.mixin_spy import SpyMixin as spy
        return {
            "GRU_SINGLETON": repr(grumod._GRU_SINGLETON),
            "SpyMixin": _cls_snap(spy),
            "InMemoryLogger": _cls_snap(InMemoryLogger),
            "InMemoryMetrics": _cls_snap(InMemoryMetrics),
            "InMemoryStateStore": _cls_snap(InMemoryStateStore),
        }

    pre = _snap()

    try:
        print("id(SpyMixin._mspy_waiters):", id(SpyMixin._mspy_waiters))
    except Exception:
        pass
    try:
        print("id(InMemoryLogger._mspy_waiters):", id(InMemoryLogger._mspy_waiters))
    except Exception:
        pass
    try:
        print("id(InMemoryMetrics._mspy_waiters):", id(InMemoryMetrics._mspy_waiters))
    except Exception:
        pass
    try:
        print("id(InMemoryStateStore._mspy_waiters):", id(InMemoryStateStore._mspy_waiters))
    except Exception:
        pass

    yield

    post = _snap()

    from pprint import pprint
    print('\n--- TEST-LEAK DIAGNOSIS (pre -> post) ---')
    pprint({"pre": pre})
    pprint({"post": post})

    grumod._GRU_SINGLETON = None
    InMemoryLogger.reset()
    InMemoryMetrics.reset()
    InMemoryStateStore.reset()

# """

@dataclass
class GruCmd():
    cmd: str
    cmd_kwargs: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not getattr(Gru, self.cmd, None):
            raise TypeError(f"Invalid Gru Command: {self.cmd}")

async def run_scenario(gru_cmds: Iterable[GruCmd]):
    # assumes pipelines only emit a single event
    # it's possible to have the user
    # declare how many events the pipeline
    # will emit but it would be nice
    # if the user could just use any pipeline
    # and not have to worry about many events
    # the pipeline emits

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset()

    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset()

    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    grumod._GRU_SINGLETON = None

    gru = await Gru.create(logger=logger, metrics=metrics, state_store=state_store)

    for gru_cmd in gru_cmds:
        # it would be nice to pull the gru code that gets
        # the domain object classes from the user strings
        # to use here instead of requiring the user to pass them too
        ...
        if gru_cmd.cmd == 'start_minion':
            ...
        elif gru_cmd.cmd == 'stop_minion':
            ...
        elif gru_cmd.cmd == 'shutdown':
            ...
        else:
            raise Exception('unsupported GruCmd')

        # verify that all ran as expected
        ...

async def setup_gru() -> tuple[Gru, InMemoryLogger, InMemoryMetrics, InMemoryStateStore]:
    InMemoryLogger.enable_spy()
    InMemoryLogger.reset()

    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset()

    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset()

    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)

    grumod._GRU_SINGLETON = None

    gru = await Gru.create(logger=logger, metrics=metrics, state_store=state_store)

    return gru, logger, metrics, state_store

async def run_gru_start_minion(
    gru: Gru,
    s_minion_modpath: str,
    minion_config_path: str,
    s_pipeline_modpath: str
) -> tuple[StartMinionResult, type[Minion], type[Pipeline]]:
    "expects paths to Minion and Pipeline that are subclasses of SpyMixin"
    minion_cls = gru._get_minion_class(s_minion_modpath)
    assert issubclass(minion_cls, SpyMixin)
    minion_cls.enable_spy()

    pipeline_cls = gru._get_pipeline_class(s_pipeline_modpath)
    assert issubclass(pipeline_cls, SpyMixin)
    pipeline_cls.enable_spy()

    result = await gru.start_minion(s_minion_modpath, minion_config_path, s_pipeline_modpath)

    return result, minion_cls, pipeline_cls

@pytest.mark.asyncio
async def test_gru_template():
    # probably better as fixtures ?
    gru, logger, metrics, state_store = await setup_gru()
    ...



@pytest.mark.asyncio
async def test_gru_does_all_it_should_helped():
    # run a pipeline w/ 1 minion
    # emit a single event
    # verify workflow spawned and all steps run

    gru, logger, metrics, state_store = await setup_gru()

    minion_modpath = "tests.assets.minion_simple"
    minion_config_path = "tests/assets/minion_config_simple_1.toml"
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    result, minion_cls, pipeline_cls = await run_gru_start_minion(gru, minion_modpath, minion_config_path, pipeline_modpath)

    assert result.success
    assert issubclass(minion_cls, SpyMixin)
    assert issubclass(pipeline_cls, SpyMixin)

    # TODO: i'm going to helperize this test fn so that way it verifies that each
    # workflow runs as expect by checking the number of steps the workflow has
    # and using the count for assertions like below

    async def await_work(minion_cls: type[Minion], pipeline_cls: type[Pipeline], state_store: InMemoryStateStore):
        assert issubclass(minion_cls, SpyMixin)
        assert issubclass(pipeline_cls, SpyMixin)
        assert minion_cls._workflow

        await pipeline_cls.wait_for_call('produce_event', count=1, timeout=1) # assuming 1 event produced

        for step in minion_cls._workflow:
            await minion_cls.wait_for_call(step.__name__, count=1, timeout=1)
            await InMemoryStateStore.wait_for_call('save_context', count=1, timeout=1)
        
        await InMemoryStateStore.wait_for_call('delete_context', count=1, timeout=1)    

    # await pipeline_cls.wait_for_call('produce_event', count=1, timeout=1)
    # await minion_cls.wait_for_call('step_1', count=1, timeout=1)
    # await InMemoryStateStore.wait_for_call('save_context', count=1, timeout=1)
    # await minion_cls.wait_for_call('step_2', count=1, timeout=1)
    # await InMemoryStateStore.wait_for_call('save_context', count=2, timeout=1)
    # await InMemoryStateStore.wait_for_call('delete_context', count=1, timeout=1)

    def verify_work(minion_cls: type[Minion], state_store: type[InMemoryStateStore]):
        assert issubclass(minion_cls, SpyMixin)
        assert minion_cls._workflow

        # verify total counts and execution order

        counts = minion_cls.get_call_counts().items()
        assert ('__init__', 1) in counts
        for step in minion_cls._workflow:
            assert (step.__name__, 1) in counts

        # this assert assume one event emit from the pipeline
        # for multiple events emit from a pipeline consider the following:
        # ... when i am testing same minion with different config files concurrently
        #     just search the _msp_count_history for N interleaved ascending sequences of length M
        #     (maybe i'll consider making it a method of SpyMixin and/or extending .assert_call_order)
        #     where N is the number of events emited from the pipeline
        #     and M is the number of steps in the workflow
        minion_cls.assert_call_order(
            ['__init__'] + list(c.__name__ for c in minion_cls._workflow)
            # maybe consider addding _startup & _shutdown
        )
        
        counts = state_store.get_call_counts().items()
        assert ('__init__', 1) in counts
        assert ('load_all_contexts', 1) in counts # happens at startup
        assert ('save_context', len(minion_cls._workflow)) in counts
        assert ('delete_context', 1) in counts # assuming 1 event emited from pipelines

        state_store.assert_call_order([ # maybe consider addding _startup & _shutdown
            '__init__',
            'load_all_contexts',
            'save_context' * (len(minion_cls._workflow) - 1),
            'delete_context'
        ])

    # counts = minion_cls.get_call_counts().items()
    # assert ('__init__', 1) in counts
    # assert ('step_1', 1) in counts
    # assert ('step_2', 1) in counts

    # counts = InMemoryStateStore.get_call_counts().items()
    # assert ('__init__', 1) in counts
    # assert ('save_context', 2) in counts # one for each state (on step1, on step2)
    # assert ('delete_context', 1) in counts
    # assert ('load_all_contexts', 1) in counts # happens at startup

    # temporary return
    return

    from pprint import pprint
    snap = metrics.snapshot()
    pprint(snap)

    # ensure we saw at least one workflow in-flight during the run
    gs = snap.get('gauges', {})
    from minions._internal._framework.metrics_constants import MINION_WORKFLOW_INFLIGHT_GAUGE
    seen = False
    for mname, buckets in gs.items():
        if mname != MINION_WORKFLOW_INFLIGHT_GAUGE:
            continue
        for labels, v in buckets.items():
            if v >= 1:
                seen = True
    assert seen, "expected to observe MINION_WORKFLOW_INFLIGHT_GAUGE >= 1"

    # basic metrics sanity: at least one snapshot is non-empty
    assert any((
        metrics.snapshot_counters(),
        metrics.snapshot_gauges(),
        metrics.snapshot_histograms(),
    ))

    # logger should have recorded something about the run
    assert hasattr(logger, "logs") and len(logger.logs) >= 1

    await gru.shutdown()

# """

@pytest.mark.asyncio
async def test_gru_does_all_it_should():
    # run a pipeline w/ 1 minion
    # emit a single event
    # verify workflow spawned and all steps run

    # for checks use:
    # - SpyMixin methods
    # - InMemoryStateStore
    # - InMemoryMetrics
    # - InMemorylogger

    # TODO: make helper/fixutes for this testing strucutre once finalized

    from tests.assets.minion_simple import SimpleMinion
    from tests.assets.pipeline_simple_single_event_1 import SimpleSingleEventPipeline1

    minion_cls = SimpleMinion
    minion_cls.enable_spy()

    pipeline_cls = SimpleSingleEventPipeline1
    pipeline_cls.enable_spy()

    minion_modpath = "tests.assets.minion_simple"
    minion_config_path = "tests/assets/minion_config_simple_1.toml"
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset()
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset()
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset()

    logger = InMemoryLogger()
    state_store = InMemoryStateStore(logger=logger)
    metrics = InMemoryMetrics()
    gru = await Gru.create(state_store=state_store, logger=logger, metrics=metrics)

    result = await gru.start_minion(minion_modpath, minion_config_path, pipeline_modpath)

    assert result.success

    await pipeline_cls.wait_for_call('produce_event', count=1, timeout=1)
    await minion_cls.wait_for_call('step_1', count=1, timeout=1)
    await InMemoryStateStore.wait_for_call('save_context', count=1, timeout=1)
    await minion_cls.wait_for_call('step_2', count=1, timeout=1)
    await InMemoryStateStore.wait_for_call('save_context', count=2, timeout=1)
    await InMemoryStateStore.wait_for_call('delete_context', count=1, timeout=1)

    # TODO: i'm going to helperize this test fn so that way it verifies that each
    # workflow runs as expect by checking the number of steps the workflow has
    # and using the count for assertions like below

    # assert minion_cls.get_call_counts() == {
    #     '__init__': 1,
    #     'step_1': 1,
    #     'step_2': 1,
    # }

    counts = minion_cls.get_call_counts().items()
    assert ('__init__', 1) in counts
    assert ('step_1', 1) in counts
    assert ('step_2', 1) in counts

    counts = InMemoryStateStore.get_call_counts().items()
    assert ('__init__', 1) in counts
    assert ('save_context', 2) in counts # one for each state (on step1, on step2)
    assert ('delete_context', 1) in counts
    assert ('load_all_contexts', 1) in counts # happens at startup

    from pprint import pprint
    snap = metrics.snapshot()
    pprint(snap)

    # ensure we saw at least one workflow in-flight during the run
    gs = snap.get('gauges', {})
    from minions._internal._framework.metrics_constants import MINION_WORKFLOW_INFLIGHT_GAUGE
    seen = False
    for mname, buckets in gs.items():
        if mname != MINION_WORKFLOW_INFLIGHT_GAUGE:
            continue
        for labels, v in buckets.items():
            if v >= 1:
                seen = True
    assert seen, "expected to observe MINION_WORKFLOW_INFLIGHT_GAUGE >= 1"

    # basic metrics sanity: at least one snapshot is non-empty
    assert any((
        metrics.snapshot_counters(),
        metrics.snapshot_gauges(),
        metrics.snapshot_histograms(),
    ))

    # logger should have recorded something about the run
    assert hasattr(logger, "logs") and len(logger.logs) >= 1

    await gru.shutdown()

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
        assert gsnap[SYSTEM_MEMORY_USED_PERCENT][()] == 55
        assert gsnap[SYSTEM_CPU_USED_PERCENT][()] == 20
        assert gsnap[PROCESS_MEMORY_USED_PERCENT][()] == 12   # 1234/10000*100
        assert gsnap[PROCESS_CPU_USED_PERCENT][()] == 2       # 16/8

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
        assert gsnap[SYSTEM_MEMORY_USED_PERCENT][()] == 55
        assert gsnap[SYSTEM_CPU_USED_PERCENT][()] == 20
        assert gsnap[PROCESS_MEMORY_USED_PERCENT][()] == 10  # 1000/10000*100
        assert gsnap[PROCESS_CPU_USED_PERCENT][()] == 2      # int(8/4)

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
        assert gsnap[SYSTEM_MEMORY_USED_PERCENT][()] == 50
        assert gsnap[SYSTEM_CPU_USED_PERCENT][()] == 10
        assert gsnap[PROCESS_MEMORY_USED_PERCENT][()] == 10
        assert gsnap[PROCESS_CPU_USED_PERCENT][()] == 2

class TestValidComposition:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_multiple_minions_and_explicit_minion(self):
            
            minion_modpath = "tests.assets.file_with_two_minions_and_explicit_minion"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=ConsoleLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert result.success

            await gru.shutdown()

        @pytest.mark.asyncio
        async def test_gru_starts_minion_with_multiple_distinct_resource_dependencies(self):
            minion_modpath = "tests.assets.minion_simple_resourced_multi"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            logger = InMemoryLogger()
            gru = await Gru.create(
                state_store=InMemoryStateStore(logger=logger),
                logger=logger,
                metrics=InMemoryMetrics()
            )

            result = await gru.start_minion(minion_modpath, config_path, pipeline_modpath)

            assert result.success

            assert len(gru._pipelines) >= 1
            assert len(gru._resources) >= 2

            assert result.instance_id is not None
            await gru.shutdown()

    class TestPipelineFile: # TODO: implement test(s)
        @pytest.mark.asyncio
        async def test_gru_accepts_file_with_single_pipeline_class(self):
            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=ConsoleLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert result.success

            await gru.shutdown()

class TestValidUsage:
    @pytest.mark.asyncio
    async def test_gru_accepts_none_logger_metrics_state_store(self):
        await Gru.create(
            logger=None,
            state_store=None,
            metrics=None
        )

    @pytest.mark.asyncio
    async def test_gru_allows_create_and_immediate_shutdown(self):
        gru = await Gru.create(
            state_store=NoOpStateStore(),
            logger=NoOpLogger(),
            metrics=NoOpMetrics()
        )
        await gru.shutdown()

    # TODO: test that pipeline event processed by minion for methods below

    @pytest.mark.asyncio
    async def test_gru_start_stop_minion(self):
        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

        tests_dir = Path(__file__).parent.parent.parent.parent
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        gru = await Gru.create(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        )

        result = await gru.start_minion(
            minion_modpath,
            config_path,
            pipeline_modpath
        )

        assert result.success
        assert result.name == "simple-minion"
        assert result.instance_id in gru._minions_by_id
        assert result.instance_id in gru._minion_tasks

        await gru.stop_minion(result.instance_id)
        await gru.shutdown()

    @pytest.mark.asyncio
    async def test_gru_start_minion_shutdown_without_stop(self):

        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

        tests_dir = Path(__file__).parent.parent.parent.parent
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        gru = await Gru.create(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        )

        result = await gru.start_minion(
            minion_modpath,
            config_path,
            pipeline_modpath
        )

        assert result.success
        assert result.name == "simple-minion"
        assert result.instance_id in gru._minions_by_id
        assert result.instance_id in gru._minion_tasks

        # skip .stop_minion method
        # await gru.stop_minion(result.instance_id)

        await gru.shutdown()
    
    # TODO: check fanouts and such for methods below

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_3_pipelines_3_resources_no_sharing(self):
        """
        Start three minions each with their own pipeline and their own Resource type
        so there is no sharing of pipelines or resources between minions.
        """
        minion1 = "tests.assets.minion_simple_resourced_1"
        minion2 = "tests.assets.minion_simple_resourced_2"
        minion3 = "tests.assets.minion_simple_resourced_3"

        pipeline1 = "tests.assets.pipeline_simple_single_event_1"
        pipeline2 = "tests.assets.pipeline_simple_single_event_2"
        pipeline3 = "tests.assets.pipeline_simple_single_event_3"

        tests_dir = Path(__file__).parent.parent.parent.parent
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        cfg2 = str(tests_dir / "assets" / "minion_config_simple_2.toml")
        cfg3 = str(tests_dir / "assets" / "minion_config_simple_3.toml")

        logger = InMemoryLogger()
        gru = await Gru.create(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        )

        r1 = await gru.start_minion(minion1, cfg1, pipeline1)
        r2 = await gru.start_minion(minion2, cfg2, pipeline2)
        r3 = await gru.start_minion(minion3, cfg3, pipeline3)

        assert r1.success and r2.success and r3.success

        # Expect three distinct pipeline IDs
        assert len(gru._pipelines) >= 3

        # Expect three distinct resource classes started
        assert len(gru._resources) >= 3

        # stop them
        assert r1.instance_id is not None
        await gru.stop_minion(r1.instance_id)
        assert r2.instance_id is not None
        await gru.stop_minion(r2.instance_id)
        assert r3.instance_id is not None
        await gru.stop_minion(r3.instance_id)

        await gru.shutdown()

    @pytest.mark.asyncio
    async def test_gru_start_3_minions_1_pipeline_1_resource_sharing(self):
        """
        Start three minions that share the same pipeline and a single Resource type.
        Verify pipeline and resource are shared and cleaned up after stopping all minions.
        """
        minion_modpath = "tests.assets.minion_simple_resourced_1"
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

        # TODO: i'm testing resource sharing between minions spawned from same minion class but different configs
        # i should also test the case where i spawn from seperate minion classes/files

        tests_dir = Path(__file__).parent.parent.parent.parent
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")
        cfg2 = str(tests_dir / "assets" / "minion_config_simple_2.toml")
        cfg3 = str(tests_dir / "assets" / "minion_config_simple_3.toml")

        # TODO: consider refactoring gru to have the kwargs be classes instead of instances
        # it might be cleaner and then the user wont have to manually wire things like this
        # and cuz then gru can handle instantiation and startup
        # but what if the use want to bring-thier-own and wants to instantiate with parameters?
        # ask copilot
        # !! will have to do the update across this whole test file !!

        logger = InMemoryLogger()
        gru = await Gru.create(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        )

        r1 = await gru.start_minion(minion_modpath, cfg1, pipeline_modpath)
        r2 = await gru.start_minion(minion_modpath, cfg2, pipeline_modpath)
        r3 = await gru.start_minion(minion_modpath, cfg3, pipeline_modpath)

        assert r1.success and r2.success and r3.success

        # pipeline should be shared (single id)
        assert len(gru._pipelines) == 1

        # resource should be shared across minions
        assert len(gru._resources) == 1

        # stop minions and assert cleanup
        assert r1.instance_id is not None
        await gru.stop_minion(r1.instance_id)
        assert len(gru._pipelines) == 1
        assert r2.instance_id is not None
        await gru.stop_minion(r2.instance_id)
        assert len(gru._pipelines) == 1
        assert r3.instance_id is not None
        await gru.stop_minion(r3.instance_id)

        # after all stopped, pipeline and resources cleaned
        assert len(gru._pipelines) == 0
        assert len(gru._resources) == 0

        await gru.shutdown()

    @pytest.mark.asyncio
    async def test_minion_and_pipeline_share_resource_dependency(self):
        minion_modpath = "tests.assets.minion_simple_resourced_1"
        pipeline_modpath = "tests.assets.pipeline_simple_resourced"

        tests_dir = Path(__file__).parent.parent.parent.parent
        cfg1 = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        logger = InMemoryLogger()
        gru = await Gru.create(
            state_store=InMemoryStateStore(logger=logger),
            logger=logger,
            metrics=InMemoryMetrics()
        )

        r1 = await gru.start_minion(minion_modpath, cfg1, pipeline_modpath)

        assert r1.success

        assert len(gru._pipelines) == 1
        assert len(gru._resources) == 1

        assert isinstance(r1.instance_id, str)
        await gru.stop_minion(r1.instance_id)

        assert len(gru._pipelines) == 0
        assert len(gru._resources) == 0

        await gru.shutdown()

    # TODO: I need tests for gru's default usages to ensure i stay version 1.x.x compliant

class TestInvalidComposition:
    class TestMinionFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_minion_file(self):

            minion_modpath = "tests.assets.file_empty"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "must define a `minion` variable or contain at least one subclass of `Minion`" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_file_with_multiple_minions_and_no_explicit_minion(self):
            
            minion_modpath = "tests.assets.file_with_two_minions"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "multiple Minion subclasses but no explicit `minion` variable" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_file_with_invalid_explicit_minion(self):

            minion_modpath = "tests.assets.file_with_invalid_explicit_minion"
            pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "is not a subclass of Minion" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_minion_workflow_context_not_serializable(self):
            minion_modpath = "tests.assets.file_with_unserializable_workflow_context_minion"
            pipeline_modpath = "tests.assets.pipeline_single_event"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "workflow context is not JSON-serializable" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio # TODO: implement
        async def test_gru_returns_error_on_minion_event_not_serializable(self):
            ...

    class TestPipelineFile:
        @pytest.mark.asyncio
        async def test_gru_returns_error_on_empty_pipeline_file(self):

            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.file_empty"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "must define a `pipeline` variable or contain at least one subclass of `Pipeline`" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio 
        async def test_gru_returns_error_on_pipeline_file_with_multiple_pipelines_and_no_explicit_pipeline(self):
            
            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.file_with_two_pipelines"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "multiple Pipeline subclasses but no explicit `pipeline` variable" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio
        async def test_gru_returns_error_on_pipeline_file_with_invalid_explicit_pipeline(self):

            minion_modpath = "tests.assets.minion_simple"
            pipeline_modpath = "tests.assets.file_with_invalid_explicit_pipeline"

            tests_dir = Path(__file__).parent.parent.parent.parent
            config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

            gru = await Gru.create(
                state_store=NoOpStateStore(),
                logger=NoOpLogger(),
                metrics=NoOpMetrics()
            )

            result = await gru.start_minion(
                minion_modpath,
                config_path,
                pipeline_modpath
            )

            assert not result.success
            assert result.reason
            assert "is not a subclass of Pipeline" in result.reason

            await gru.shutdown()

        @pytest.mark.asyncio # TODO: implement
        async def test_gru_returns_error_on_pipeline_event_not_serializable(self):...

    # Resource doesn't have tests like in TestMinion and TestPipeline
    # because Resources dependencies are declared as type hints
    # when creating Minion and Pipeline subclasses.

    # TODO: ensure gru properly handles Minions and Pipelines with multiple Resource dependency declarations

class TestInvalidUsage:
    @pytest.mark.asyncio
    async def test_gru_raises_on_direct_instantiation(self):
        with pytest.raises(RuntimeError):
            Gru(
                loop=asyncio.get_running_loop(),
                logger=NoOpLogger(),
                state_store=NoOpStateStore(),
                metrics=NoOpMetrics()
            )

    @pytest.mark.asyncio
    async def test_gru_raises_on_multiple_instances(self):
        gru = await Gru.create(
            logger=NoOpLogger(),
            metrics=NoOpMetrics(),
            state_store=NoOpStateStore()
        )
        with pytest.raises(RuntimeError, match="Only one Gru instance is allowed per process."):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore()
            )
        await gru.shutdown()

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_logger", [123, "invalid"])
    async def test_gru_raises_on_invalid_logger_param(self, bad_logger):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=bad_logger,
                metrics=NoOpMetrics(),
                state_store=NoOpStateStore()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_metrics", [123, "invalid"])
    async def test_gru_raises_on_invalid_metrics_param(self, bad_metrics):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=bad_metrics,
                state_store=NoOpStateStore()
            )

    @pytest.mark.asyncio
    @pytest.mark.parametrize("bad_state_store", [123, "invalid"])
    async def test_gru_raises_on_invalid_state_store_param(self, bad_state_store):
        with pytest.raises(TypeError):
            await Gru.create(
                logger=NoOpLogger(),
                metrics=NoOpMetrics(),
                state_store=bad_state_store
            )

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_starting_running_minion(self):
        # TODO:
        # - start 2 minions with the same name (would need to start different minion but give the same name)
        # - stop minion by name => and get error as a value
        # TODO: but actually runs with events be created and stuff?

        print('--------- start problematic test ---------')

        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

        tests_dir = Path(__file__).parent.parent.parent.parent
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        gru = await Gru.create(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        )

        result1 = await gru.start_minion(
            minion_modpath,
            config_path,
            pipeline_modpath
        )

        print(result1)

        assert result1.success
        assert result1.name == "simple-minion"
        assert result1.instance_id in gru._minions_by_id
        assert result1.instance_id in gru._minion_tasks

        result2 = await gru.start_minion(
            minion_modpath,
            config_path,
            pipeline_modpath
        )

        print(result2)

        assert not result2.success
        assert result2.reason
        assert "Minion already running" in result2.reason

        await gru.shutdown()

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_stopping_nonexistant_minion(self):

        gru = await Gru.create(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        )

        result = await gru.stop_minion('mock') 

        print(result)

        assert not result.success
        assert result.reason
        assert "No minion found with the given name or instance ID" in result.reason

        await gru.shutdown()

    @pytest.mark.asyncio
    async def test_gru_returns_error_when_mismatched_minion_and_pipeline_event_types(self):

        minion_modpath = "tests.assets.minion_simple"
        pipeline_modpath = "tests.assets.pipeline_dict"

        tests_dir = Path(__file__).parent.parent.parent.parent
        config_path = str(tests_dir / "assets" / "minion_config_simple_1.toml")

        gru = await Gru.create(
            state_store=NoOpStateStore(),
            logger=ConsoleLogger(),
            metrics=NoOpMetrics()
        )

        result = await gru.start_minion(
            minion_modpath,
            config_path,
            pipeline_modpath
        )

        print(result)

        assert not result.success
        assert result.reason
        assert "Incompatible minion and pipeline event types" in result.reason

        await gru.shutdown()


    # would need to be run in gru ...
    # def test_invalid_user_code_in_step(self):
    #     with pytest.raises(Exception):
    #         class MyMinion(Minion[dict,dict]):
    #             @minion_step
    #             async def step_1(self):
    #                 import asyncio
    #                 async def _(): ...
    #                 asyncio.create_task(_())
                    