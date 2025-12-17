import asyncio
import importlib
import inspect
import types
import pytest
import pytest_asyncio
import sys

from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

import minions._internal._domain.gru as grumod

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion
from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.resource import Resource

from tests.assets.support.mixin_spy import SpyMixin
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource

from minions._internal._framework.logger_console import ConsoleLogger
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from minions._internal._framework.logger import DEBUG, INFO, WARNING, ERROR, CRITICAL
from minions._internal._framework.metrics_constants import (
    SYSTEM_MEMORY_USED_PERCENT, SYSTEM_CPU_USED_PERCENT,
    PROCESS_MEMORY_USED_PERCENT, PROCESS_CPU_USED_PERCENT
)


# --- Constants ---

TESTS_DIR = Path(__file__).resolve().parents[3]


# --- Fixtures ---

@pytest.fixture(autouse=True)
def scrub_asset_modules():
    "enables non-leaky reuse of assets across tests"
    importlib.invalidate_caches()

    def is_asset_module(name: str) -> bool:
        return name.startswith("tests.assets.") and not name.startswith("tests.assets.support")

    for name in [n for n in list(sys.modules) if is_asset_module(n)]:
        sys.modules.pop(name, None)

    yield

    for name in [n for n in list(sys.modules) if is_asset_module(n)]:
        sys.modules.pop(name, None)

@pytest.fixture
def logger():
    InMemoryLogger.enable_spy()
    InMemoryLogger.reset()
    return InMemoryLogger()

@pytest.fixture
def metrics():
    InMemoryMetrics.enable_spy()
    InMemoryMetrics.reset()
    return InMemoryMetrics()

@pytest.fixture
def state_store(logger):
    InMemoryStateStore.enable_spy()
    InMemoryStateStore.reset()
    return InMemoryStateStore(logger=logger)

@pytest_asyncio.fixture
async def gru(logger, metrics, state_store):
    grumod._GRU_INSTANCE = None
    g = await Gru.create(
        logger=logger,
        metrics=metrics,
        state_store=state_store,
    )
    try:
        yield g
    finally:
        await g.shutdown()
        grumod._GRU_INSTANCE = None

# from prometheus_client import REGISTRY
# def reset_prometheus_registry():
#     collectors = list(REGISTRY._collector_to_names.keys())
#     for c in collectors:
#         try:
#             REGISTRY.unregister(c)
#         except KeyError:
#             pass

# --- Helpers (draft) ---

@dataclass(frozen=True)
class MinionStart:
    expect_success: bool
    minion_modpath: str
    minion_config_path: str
    pipeline_modpath: str

    def as_kwargs(self):
        allowed = set(inspect.signature(Gru.start_minion).parameters)
        return {k: v for k, v in self.__dict__.items() if k in allowed}

@dataclass(frozen=True)
class MinionStop:
    expect_success: bool
    name_or_instance_id: str

    def as_kwargs(self):
        allowed = set(inspect.signature(Gru.stop_minion).parameters)
        return {k: v for k, v in self.__dict__.items() if k in allowed}

@pytest.fixture
def start_and_validate_minions(gru: Gru, logger: InMemoryLogger, metrics: InMemoryMetrics, state_store: InMemoryStateStore):
    async def _fn(starts: list[MinionStart], parallel_starts=True):
        # TODO: refactor this without the pipeline emits 1 event assumption
        # TODO: assert __init__, startup, run, shutdown callbacks for pipelines, minions, and resources
        # TODO: handle workflow failures and abortions, currently we assume workflows complete successfully
        # TODO: simulate and handle workflow restarts

        unique_minion_classes: dict[str, type[SpiedMinion]] = {}     # key = modpath
        unique_pipeline_classes: dict[str, type[SpiedPipeline]] = {} # key = modpath
        unique_resource_classes: set[type[SpiedResource]] = set()
        # unique_resource_classes: dict[str, type[SpiedResource]] = {} # key = idk yet (might just make a set)

        pipeline_sentries: list[asyncio.Task] = [] # enforce that pipelines emit only one event
        
        try:
            # --- Setup and Execute Starts ---

            for s in starts:
                if s.minion_modpath not in unique_minion_classes:
                    m_cls = gru._get_minion_class(s.minion_modpath)
                    assert issubclass(m_cls, SpiedMinion)
                    m_cls.enable_spy()
                    unique_minion_classes[s.minion_modpath] = m_cls

                    deps = gru._get_resource_dependencies(m_cls)
                    if deps:
                        true_deps = deps + [
                            x for d in deps
                            for x in gru._get_resource_dependencies(d)
                        ]
                        for td in set(true_deps):
                            assert issubclass(td, SpiedResource)
                            td.enable_spy()
                            unique_resource_classes.add(td)
                
                if s.pipeline_modpath not in unique_pipeline_classes:
                    p_cls = gru._get_pipeline_class(s.pipeline_modpath)
                    assert issubclass(p_cls, SpiedPipeline)
                    p_cls.enable_spy()
                    unique_pipeline_classes[s.pipeline_modpath] = p_cls

                    deps = gru._get_resource_dependencies(p_cls)
                    if deps:
                        true_deps = deps + [
                            x for d in deps
                            for x in gru._get_resource_dependencies(d)
                        ]
                        for td in set(true_deps):
                            assert issubclass(td, SpiedResource)
                            td.enable_spy()
                            unique_resource_classes.add(td)
            
            async def _enforce_single_event(pipeline_cls: type[SpiedPipeline]) -> None:
                try:
                    await pipeline_cls.wait_for_calls(expected={'produce_event': 2}, timeout=3600)
                    pytest.fail(f"{pipeline_cls.__name__} emitted more than one event (expected singleton pipeline with a single event)")
                except asyncio.CancelledError:
                    pass
            
            for pipeline_cls in unique_pipeline_classes.values():
                pipeline_sentries.append(asyncio.create_task(_enforce_single_event(pipeline_cls)))
            
            results = await asyncio.gather(*[
                gru.start_minion(**s.as_kwargs()) for s in starts
            ]) if parallel_starts else [
                await gru.start_minion(**s.as_kwargs()) for s in starts
            ]
            
            minion_started_counts: defaultdict[type[SpiedMinion], int] = defaultdict(int) # successful_minion_start_counts: dict[type[SpiedMinion], int] = {}

            for s, r in zip(starts, results):
                assert r.success == s.expect_success, f"result.success != {s.expect_success} for kwargs={s.as_kwargs()}"
                if r.success:
                    minion_started_counts[unique_minion_classes[s.minion_modpath]] += 1

            # --- Define Expected Call Counts and Call Orders ---

            # It isn't possible to reliably determine how many time each resource specific endpoint (user public funcs) will be run before

            call_counts: dict[type[SpyMixin], dict[str, int]] = {}
            call_orders: dict[type[SpyMixin], tuple[str, ...]] = {}

            for r_cls in unique_resource_classes:
                call_counts[r_cls] = {
                    '__init__': 1,
                    'startup': 1,
                    'run': 1,
                    # Determining resource specifc endpoint call counts (user public funcs) before executing an orchestration scenario in unreliable.
                    # Though I have a reliable count how how many times each of them are called given an orchestration scenario.
                    # So I'll do call count asserts for a straight forward orchestration scenarios and just trust that
                    # that asserted behavior is carried through to all other orchestration scenarios.
                    # TODO: when i write test(s):
                    # - i'll get the resources manually
                    # - run start_and_validate_minions
                    # - assert resource call counts
                    # - run stop_and_validate_minions?
                    # TODO: i should probably split the start and stop logic
                    # into two different functions
                    # unlike now they are kind of baked together
                    'shutdown': 1
                }
                call_orders[r_cls] = tuple(call_counts[r_cls].keys())

            for p_cls in unique_pipeline_classes.values():
                call_counts[p_cls] = {
                    '__init__': 1,
                    'startup': 1,
                    'run': 1,
                    'produce_event': 1,
                    'shutdown': 1
                }
                call_orders[p_cls] = tuple(call_counts[p_cls].keys())

            for m_cls in unique_minion_classes.values():
                call_counts[m_cls] = {
                    '__init__': 1,
                    'startup': 1,
                    'run': 1,
                    **{name: minion_started_counts[m_cls] for name in m_cls._mn_workflow_spec},  # type: ignore
                    'shutdown': 1
                }
                call_orders[m_cls] = tuple(call_counts[m_cls].keys())

            call_counts[type(state_store)] = {
                '__init__': 1,
                'startup': 1,
                'run': 1,
                'load_all_contexts': sum(minion_started_counts.values()),
                'save_context': sum(
                    len(m_cls._mn_workflow_spec) * minion_started_counts[m_cls] # type: ignore
                    for m_cls in unique_minion_classes.values()
                ),
                'delete_context': sum(minion_started_counts.values()),
                'shutdown': 1
            }
            call_orders[type(state_store)] = tuple(call_counts[type(state_store)].keys())

            # TODO: define metrics call count / order
            call_counts[type(metrics)] = {}
            call_orders[type(metrics)] = tuple(call_counts[type(metrics)].keys())

            # TODO: define logger call count / order
            call_counts[type(logger)] = {}
            call_orders[type(logger)] = tuple(call_counts[type(logger)].keys())

            # --- Await and Pin Call Counts ---

            await asyncio.gather(*[
                cls.await_and_pin_call_counts(
                    expected=counts,
                    on_extra=lambda *a, _cls=cls, **kw: pytest.fail(
                        f"Unexpected extra calls for {_cls.__name__}: args={a} kwargs={kw}"
                    ),
                    allow_unlisted=True,
                    timeout=5
                )
                for cls, counts in call_counts.items()
            ])

            # --- Assert Call Orders (per instance) ---

            # TODO: write properly
            # for minion_cls in minion_started_counts.keys():
            #     assert minion_cls._mn_workflow_spec, "workflow spec must not be empty"
            #     for i in range(1, minion_started_counts[minion_cls] + 1):
            #         minion_cls.assert_call_order_for_instance(
            #             i, ['__init__', *list(minion_cls._mn_workflow_spec)]
            #         )

        finally:
            for cls in unique_minion_classes.values():
                cls.reset()
            for cls in unique_pipeline_classes.values():
                cls.reset()
            for cls in unique_resource_classes:
                cls.reset()
            for t in pipeline_sentries:
                t.cancel()
            await asyncio.gather(*pipeline_sentries, return_exceptions=True)

    return _fn

@pytest.fixture
def stop_and_validate_minions(gru: Gru, logger: InMemoryLogger, metrics: InMemoryMetrics, state_store: InMemoryStateStore):
    async def _fn(stops: list[MinionStop], parallel_stops=True):
        ...
    return _fn

# implement a stop_and_validate_minion helper?
# then that way i can start and stop minions
# how ever i please / in any order
# and i can robustly tests various scenarios
# maybe even going gru.shutdown before stopping all minions
# I probably need to implement a shutdown_and_validate gru helper too

# --- Helpers ---

class Directive(): ...

@dataclass(frozen=True)
class MinionStart(Directive):
    minion_modpath: str
    minion_config_path: str
    pipeline_modpath: str

    expect_success: bool
    workflow_resolutions: dict[str, int] # {'succeeded':1, 'failed':1, 'aborted':1}

    def as_kwargs(self):
        allowed = set(inspect.signature(Gru.start_minion).parameters)
        return {k: v for k, v in self.__dict__.items() if k in allowed}

@dataclass(frozen=True)
class MinionStop(Directive):
    expect_success: bool
    name_or_instance_id: str

    def as_kwargs(self):
        allowed = set(inspect.signature(Gru.stop_minion).parameters)
        return {k: v for k, v in self.__dict__.items() if k in allowed}

@dataclass(frozen=True)
class GruShutdown(Directive):
    ...

@pytest.fixture
async def run_orchestration(gru: Gru, logger: InMemoryLogger, metrics: InMemoryMetrics, state_store: InMemoryStateStore):
    async def _fn(
        directives: list[Directive],
        *,
        parallelize_starts: bool = True,
        parallelize_stops: bool = True,
        enforce_single_event: bool = True,
        timeout: float = 5.0,
    ) -> None:
        # DesignNote:
        # - it's fine to require the tester to declare the number of events the pipeline will emit.
        # - i will shutdown pipelines only after they finish emitting all thier events as declared in the directive/test
        #   because shutting down before then doesn't add test value / doesn't test anything extra about the codebase.

        # TODO: assert pipeline event counts with a kwargs like pipeline_event_counts={'modpath.to.pipeline':1}
        # TODO: assert workflow resolutions with a kwargs on StartMinion like wf_resolutions={'succeeded':1, 'failed':1, 'aborted':1}
        # TODO: simulate and validate workflow restarts

        # TODO: i'm writing this run orchestration "test engine" and writing tests with it
        # currently, i'm writing it assuming in-memory statestore, metrics, logger, etc. because it's fast and easy to validate
        # but i should work on how to make run_orchestration and tests that use it
        # agnostic in the sense that they take the base classes of the domain objects (like StateStore, Metrics, etc.) as args
        # so that way, I can test any super of them (mine or users that implement thier own) against
        # the entire test suite i have developed for minions framework. that's a powerful decision.

        unique_minions: dict[str, type[SpiedMinion]] = {}
        unique_pipelines: dict[str, type[SpiedPipeline]] = {}
        unique_resources: set[type[SpiedResource]] = set()

        sentries: list[asyncio.Task] = []

        try:
            # Discover classes for all MinionStart directives and enable spies.
            for d in directives:
                if not isinstance(d, MinionStart):
                    continue

                if d.minion_modpath not in unique_minions:
                    m = gru._get_minion_class(d.minion_modpath)
                    assert issubclass(m, SpiedMinion)
                    m.enable_spy()
                    unique_minions[d.minion_modpath] = m

                    for r in gru._get_all_resource_dependencies(m):
                        if r not in unique_resources:
                            assert issubclass(r, SpiedResource)
                            r.enable_spy()
                            unique_resources.add(r)

                if d.pipeline_modpath not in unique_pipelines:
                    p = gru._get_pipeline_class(d.pipeline_modpath)
                    assert issubclass(p, SpiedPipeline)
                    p.enable_spy()
                    unique_pipelines[d.pipeline_modpath] = p

                    for r in gru._get_all_resource_dependencies(p):
                        if r not in unique_resources:
                            assert issubclass(r, SpiedResource)
                            r.enable_spy()
                            unique_resources.add(r)

            async def _sentry(p_cls: type[SpiedPipeline]):
                try:
                    await p_cls.await_and_pin_call_counts(
                        expected={'produce_event': 1},
                        on_extra=pytest.fail(f"{p_cls.__name__} emitted > 1 event"),
                        allow_unlisted=True,
                        timeout=float('inf')
                    )
                except asyncio.CancelledError:
                    pass
            for p in unique_pipelines.values():
                sentries.append(asyncio.create_task(_sentry(p)))

            # Execute directives in order, batching parallel starts/stops.
            start_batch: list[MinionStart] = []
            stop_batch: list[MinionStop] = []
            seen_shutdown = False # TODO: raise if more than on shutdown

            started_counts: defaultdict[type[SpiedMinion], int] = defaultdict(int)

            async def flush_starts():
                nonlocal start_batch
                if not start_batch:
                    return
                if parallelize_starts:
                    results = await asyncio.gather(*[gru.start_minion(**d.as_kwargs()) for d in start_batch])
                else:
                    results = [await gru.start_minion(**d.as_kwargs()) for d in start_batch]
                for d, r in zip(start_batch, results):
                    assert r.success == d.expect_success, f"start_minion mismatch: {d} -> {r}"
                    if r.success:
                        m_cls = unique_minions[d.minion_modpath]
                        started_counts[m_cls] += 1
                start_batch = []

            async def flush_stops():
                nonlocal stop_batch
                if not stop_batch:
                    return
                if parallelize_stops:
                    results = await asyncio.gather(*[gru.stop_minion(**d.as_kwargs()) for d in stop_batch])
                else:
                    results = [await gru.stop_minion(**d.as_kwargs()) for d in stop_batch]
                for d, r in zip(stop_batch, results):
                    assert r.success == d.expect_success, f"stop_minion mismatch: {d} -> {r}"
                stop_batch = []

            for d in directives:
                if isinstance(d, MinionStart):
                    # accumulate consecutive starts; do not mix with stops
                    if stop_batch:
                        await flush_stops()
                    start_batch.append(d)

                elif isinstance(d, MinionStop):
                    # accumulate consecutive stops; do not mix with starts
                    if start_batch:
                        await flush_starts()
                    stop_batch.append(d)

                elif isinstance(d, GruShutdown):
                    # close any open batches, then shutdown, then continue
                    await flush_starts()
                    await flush_stops()
                    await gru.shutdown()
                    seen_shutdown = True

                else:
                    pytest.fail(f"Unknown directive: {d!r}")

            # flush any trailing batch
            await flush_starts()
            await flush_stops()

            # ---- Validation (always on) ----
            call_counts: dict[type[SpyMixin], dict[str, int]] = {}
            call_orders: dict[type[SpyMixin], tuple[str, ...]] = {}

            # Resources: lifecycle only (endpoint counts are scenario-dependent)
            for r in unique_resources:
                base = {'__init__':1,'startup':1,'run':1}
                if seen_shutdown: base['shutdown'] = 1
                call_counts[r] = base
                call_orders[r] = tuple(base)

            # Pipelines (deterministic)
            for p in unique_pipelines.values():
                base = {'__init__':1,'startup':1,'run':1,'produce_event':1}
                if seen_shutdown: base['shutdown'] = 1
                call_counts[p] = base
                call_orders[p] = tuple(base)

            # Minions (deterministic given started_counts + workflow spec)
            for m in unique_minions.values():
                base = {'__init__':1,'startup':1,'run':1,
                        **{name: started_counts[m] for name in m._mn_workflow_spec}}  # type: ignore
                if seen_shutdown: base['shutdown'] = 1
                call_counts[m] = base
                call_orders[m] = tuple(base)

            # State store (deterministic)
            ss = {'__init__':1,'startup':1,'run':1,
                  'load_all_contexts': sum(started_counts.values()),
                  'save_context': sum(len(m._mn_workflow_spec)*started_counts[m] for m in unique_minions.values()),  # type: ignore
                  'delete_context': sum(started_counts.values())}
            if seen_shutdown: ss['shutdown'] = 1
            call_counts[type(state_store)] = ss
            call_orders[type(state_store)] = tuple(ss)

            # TODO: add metrics and logger checks here like above
            metrics._snapshot
            metrics.snapshot

            # Await & pin
            await asyncio.gather(*[
                cls.await_and_pin_call_counts(
                    expected=counts,
                    on_extra=lambda *a, _cls=cls, **kw: pytest.fail(
                        f"Unexpected extra calls for {_cls.__name__}: args={a} kwargs={kw}"
                    ),
                    allow_unlisted=(cls in unique_resources),  # only resources allow unlisted endpoints
                    timeout=timeout,
                )
                for cls, counts in call_counts.items()
            ])

        finally:
            for t in sentries:
                t.cancel()
            await asyncio.gather(*sentries, return_exceptions=True)
            # Optionally reset spies here, or leave to the caller if they want to inspect histories.

    return _fn

# TODO: rewrite below tests to use the above helpers where it makes sense

# --- Tests ---

# @pytest.mark.asyncio
# async def test_example_using_helper(start_and_validate_minions):
#     starts: list[MinionStart] = [
#         MinionStart(
#             minion_modpath="tests.assets.minion_simple",
#             minion_config_path=str(TESTS_DIR / "assets" / "minion_config_simple_1.toml"),
#             pipeline_modpath="tests.assets.pipeline_simple_single_event_1",
#             expect_success=True,
#         ),
#     ]
#     await start_and_validate_minions(starts)

# TODO: replace below NoOp(s) with below InMemory(s) w/ robust checks
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore

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

    class TestPipelineFile: # TODO: implement test(s) (maybe like in TestMinionFile?)
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
    # NOTE: should have test for each case where gru
    # returns error given invalid minion, pipeline, resource

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
                    