import asyncio
import inspect
import pytest
from dataclasses import dataclass

class Directive(): ...

@dataclass(frozen=True)
class MinionStart(Directive):
    expect_success: bool
    minion_modpath: str
    minion_config_path: str
    pipeline_modpath: str

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
        uniq_minions: dict[str, type[SpiedMinion]] = {}
        uniq_pipes: dict[str, type[SpiedPipeline]] = {}
        uniq_res: set[type[SpiedResource]] = set()
        sentries: list[asyncio.Task] = []
        started_counts: defaultdict[type[SpiedMinion], int] = defaultdict(int)

        def transitive_resources(cls) -> set[type[SpiedResource]]:
            seen, stack = set(), list(gru._get_resource_dependencies(cls) or [])
            while stack:
                c = stack.pop()
                if c in seen: continue
                seen.add(c)
                stack.extend(gru._get_resource_dependencies(c) or [])
            return seen

        try:
            # Discover classes for all MinionStart directives and enable spies.
            for d in directives:
                if isinstance(d, MinionStart):
                    if d.minion_modpath not in uniq_minions:
                        m = gru._get_minion_class(d.minion_modpath); assert issubclass(m, SpiedMinion); m.enable_spy()
                        uniq_minions[d.minion_modpath] = m
                        for r in transitive_resources(m):
                            assert issubclass(r, SpiedResource); r.enable_spy(); uniq_res.add(r)
                    if d.pipeline_modpath not in uniq_pipes:
                        p = gru._get_pipeline_class(d.pipeline_modpath); assert issubclass(p, SpiedPipeline); p.enable_spy()
                        uniq_pipes[d.pipeline_modpath] = p
                        for r in transitive_resources(p):
                            assert issubclass(r, SpiedResource); r.enable_spy(); uniq_res.add(r)

            if enforce_single_event:
                async def _guard(p_cls: type[SpiedPipeline]):
                    try:
                        await p_cls.wait_for_calls(expected={'produce_event': 2}, timeout=3600)
                        pytest.fail(f"{p_cls.__name__} emitted >1 event")
                    except asyncio.CancelledError:
                        pass
                for p in uniq_pipes.values():
                    sentries.append(asyncio.create_task(_guard(p)))

            # Execute directives in order, batching parallel starts/stops.
            start_batch: list[MinionStart] = []
            stop_batch: list[MinionStop] = []
            seen_shutdown = False # TODO: raise if more than on shutdown

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
                        m_cls = uniq_minions[d.minion_modpath]
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
            for r in uniq_res:
                base = {'__init__':1,'startup':1,'run':1}
                if seen_shutdown: base['shutdown'] = 1
                call_counts[r] = base
                call_orders[r] = tuple(base)

            # Pipelines (deterministic)
            for p in uniq_pipes.values():
                base = {'__init__':1,'startup':1,'run':1,'produce_event':1}
                if seen_shutdown: base['shutdown'] = 1
                call_counts[p] = base
                call_orders[p] = tuple(base)

            # Minions (deterministic given started_counts + workflow spec)
            for m in uniq_minions.values():
                base = {'__init__':1,'startup':1,'run':1,
                        **{name: started_counts[m] for name in m._mn_workflow_spec}}  # type: ignore
                if seen_shutdown: base['shutdown'] = 1
                call_counts[m] = base
                call_orders[m] = tuple(base)

            # State store (deterministic)
            ss = {'__init__':1,'startup':1,'run':1,
                  'load_all_contexts': sum(started_counts.values()),
                  'save_context': sum(len(m._mn_workflow_spec)*started_counts[m] for m in uniq_minions.values()),  # type: ignore
                  'delete_context': sum(started_counts.values())}
            if seen_shutdown: ss['shutdown'] = 1
            call_counts[type(state_store)] = ss
            call_orders[type(state_store)] = tuple(ss)

            # Await & pin
            await asyncio.gather(*[
                cls.await_and_pin_call_counts(
                    expected=counts,
                    on_extra=lambda *a, _cls=cls, **kw: pytest.fail(
                        f"Unexpected extra calls for {_cls.__name__}: args={a} kwargs={kw}"
                    ),
                    allow_unlisted=(cls in uniq_res),  # only resources allow unlisted endpoints
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
