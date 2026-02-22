import asyncio
from collections import defaultdict
from dataclasses import dataclass, field

import pytest

from minions._internal._domain.gru import Gru
from minions._internal._domain.minion import Minion

from tests.assets.support.mixin_spy import SpyMixin
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource

try:
    from tests.assets.support.minion_spied import SpiedMinion as LegacySpiedMinion
    from tests.assets.support.pipeline_spied import SpiedPipeline as LegacySpiedPipeline
    from tests.assets.support.resource_spied import SpiedResource as LegacySpiedResource
except Exception:  # pragma: no cover - legacy assets may be removed later
    LegacySpiedMinion = None
    LegacySpiedPipeline = None
    LegacySpiedResource = None

from .directives import (
    Concurrent,
    Directive,
    GruShutdown,
    MinionStart,
    MinionStop,
    WaitWorkflows,
    iter_directives_flat,
)
from .introspect import GruIntrospector
from .plan import ScenarioPlan


@dataclass
class SpyRegistry:
    minions: dict[str, type[SpiedMinion]] = field(default_factory=dict)
    pipelines: dict[str, type[SpiedPipeline]] = field(default_factory=dict)
    resources: set[type[SpiedResource]] = field(default_factory=set)
    pipeline_start_attempt_counts: defaultdict[str, int] = field(default_factory=lambda: defaultdict(int))

    def enable_and_reset(self) -> None:
        for m in self.minions.values():
            m.enable_spy()
            m.reset_spy()
        for p in self.pipelines.values():
            p.enable_spy()
            p.reset_spy()
        for r in self.resources:
            r.enable_spy()
            r.reset_spy()


@dataclass(frozen=True)
class StartReceipt:
    directive_index: int
    minion_modpath: str
    pipeline_modpath: str
    instance_id: str | None
    resolved_name: str | None
    minion_cls: type[SpiedMinion] | None
    success: bool


@dataclass
class ScenarioRunResult:
    seen_shutdown: bool = False
    spies: SpyRegistry | None = None
    started_minions: set[Minion] = field(default_factory=set)
    instance_tags: defaultdict[type[SpyMixin], set[int]] = field(default_factory=lambda: defaultdict(set))
    extra_calls: list[tuple[type[SpyMixin], tuple, dict]] = field(default_factory=list)
    receipts: list[StartReceipt] = field(default_factory=list)


class ScenarioRunner:
    def __init__(
        self,
        gru: Gru,
        plan: ScenarioPlan,
        *,
        per_verification_timeout: float,
    ):
        self._gru = gru
        self._plan = plan
        self._insp = GruIntrospector(gru)
        self._timeout = per_verification_timeout
        self._spies = SpyRegistry()
        self._result: ScenarioRunResult | None = None

    async def run(self) -> ScenarioRunResult:
        self._discover_spies()
        self._spies.enable_and_reset()
        self._result = ScenarioRunResult()
        self._result.spies = self._spies
        for d in self._plan.directives:
            await self._execute(d)
        return self._result

    def _discover_spies(self) -> None:
        minion_bases = tuple(
            base for base in (SpiedMinion, LegacySpiedMinion) if base is not None
        )
        pipeline_bases = tuple(
            base for base in (SpiedPipeline, LegacySpiedPipeline) if base is not None
        )
        resource_bases = tuple(
            base for base in (SpiedResource, LegacySpiedResource) if base is not None
        )

        for d in iter_directives_flat(self._plan.directives):
            if not isinstance(d, MinionStart):
                continue

            self._spies.pipeline_start_attempt_counts[d.pipeline] += 1

            if d.minion not in self._spies.minions:
                m_cls = self._insp.get_minion_class(d.minion)
                assert minion_bases and issubclass(m_cls, minion_bases)
                self._spies.minions[d.minion] = m_cls

                for r_cls in self._insp.get_all_resource_dependencies(m_cls):
                    assert resource_bases and issubclass(r_cls, resource_bases)
                    self._spies.resources.add(r_cls)

            if d.pipeline not in self._spies.pipelines:
                p_cls = self._insp.get_pipeline_class(d.pipeline)
                assert pipeline_bases and issubclass(p_cls, pipeline_bases)
                self._spies.pipelines[d.pipeline] = p_cls

                for r_cls in self._insp.get_all_resource_dependencies(p_cls):
                    assert resource_bases and issubclass(r_cls, resource_bases)
                    self._spies.resources.add(r_cls)

    async def _execute(self, d: Directive) -> None:
        if isinstance(d, Concurrent):
            await asyncio.gather(*[self._execute(child) for child in d.directives])

        elif isinstance(d, MinionStart):
            await self._run_start(d)

        elif isinstance(d, MinionStop):
            await self._run_stop(d)

        elif isinstance(d, WaitWorkflows):
            await self._wait_workflows(d.minion_names)

        elif isinstance(d, GruShutdown):
            await self._run_shutdown(d)

        else:
            pytest.fail(f"Unknown directive: {d!r}")

    async def _run_start(self, d: MinionStart) -> None:
        result = self._require_result()
        r = await self._gru.start_minion(**d.as_kwargs())
        if r.success != d.expect_success:
            raise AssertionError(f"start_minion mismatch: {d} -> {r}")

        receipt = StartReceipt(
            directive_index=self._plan.directive_index(d),
            minion_modpath=d.minion,
            pipeline_modpath=d.pipeline,
            instance_id=getattr(r, "instance_id", None),
            resolved_name=getattr(r, "name", None),
            minion_cls=None,
            success=r.success,
        )

        if not r.success:
            result.receipts.append(receipt)
            return

        minion_inst = None
        if receipt.instance_id:
            minion_inst = self._insp.get_minion_instance(receipt.instance_id)

        if minion_inst is not None:
            resolved_name = getattr(minion_inst, "_mn_name", receipt.resolved_name)
            m_cls = type(minion_inst)
            minion_cls = m_cls if issubclass(m_cls, SpiedMinion) else None
            receipt = StartReceipt(
                **{**receipt.__dict__, "resolved_name": resolved_name, "minion_cls": minion_cls}
            )
            result.started_minions.add(minion_inst)

        result.receipts.append(receipt)
        self._record_instance_tags(minion_inst, d.pipeline, receipt.instance_id)

    async def _run_stop(self, d: MinionStop) -> None:
        r = await self._gru.stop_minion(**d.as_kwargs())
        if r.success != d.expect_success:
            raise AssertionError(f"stop_minion mismatch: {d} -> {r}")

    async def _run_shutdown(self, d: GruShutdown) -> None:
        result = self._require_result()
        r = await self._gru.shutdown()
        if r.success != d.expect_success:
            raise AssertionError(f"shutdown mismatch: expected {d.expect_success}, got {r}")
        result.seen_shutdown = r.success

    async def _wait_workflows(self, minion_names: set[str] | None) -> None:
        waiter = ScenarioWaiter(
            self._plan,
            self._insp,
            self._timeout,
            self._spies,
            result=self._require_result(),
        )
        await waiter.wait(minion_names=minion_names)

    def _record_instance_tags(
        self,
        minion_inst: Minion | None,
        pipeline_modpath: str,
        instance_id: str | None,
    ) -> None:
        self._record_tag_if_present(minion_inst)
        if instance_id is None:
            return

        pipeline_inst = self._insp.get_pipeline_instance(pipeline_modpath)
        self._record_tag_if_present(pipeline_inst)

        resource_ids = self._insp.resource_ids_for(
            minion_instance_id=instance_id,
            pipeline_modpath=pipeline_modpath,
        )
        for rid in resource_ids:
            res_inst = self._insp.get_resource_instance(rid)
            self._record_tag_if_present(res_inst)

    def _record_tag_if_present(self, inst: object | None) -> None:
        if inst is None:
            return
        tag = getattr(inst, "_mspy_instance_tag", None)
        if tag is None:
            return
        result = self._require_result()
        result.instance_tags[type(inst)].add(tag)

    def _require_result(self) -> ScenarioRunResult:
        if self._result is None:
            raise AssertionError("ScenarioRunner result is unavailable before run() starts.")
        return self._result


class ScenarioWaiter:
    def __init__(
        self,
        plan: ScenarioPlan,
        insp: GruIntrospector,
        timeout: float,
        spies: SpyRegistry,
        *,
        result: ScenarioRunResult,
    ):
        self._plan = plan
        self._insp = insp
        self._timeout = timeout
        self._spies = spies
        self._result = result

    async def wait(self, *, minion_names: set[str] | None) -> None:
        await self._wait_expected_workflow_calls(minion_names=minion_names)
        await self._wait_minion_tasks(self._result.started_minions)

    async def _wait_expected_workflow_calls(
        self,
        *,
        minion_names: set[str] | None,
    ) -> None:
        if minion_names is not None and not minion_names:
            return

        expected_per_class: defaultdict[type[SpiedMinion], int] = defaultdict(int)

        if minion_names is None:
            self._add_expected_for_receipts(expected_per_class, self._result.receipts)
        else:
            missing: list[str] = []
            for name in minion_names:
                receipts = [r for r in self._result.receipts if r.resolved_name == name]
                if receipts:
                    self._add_expected_for_receipts(expected_per_class, receipts)
                    continue
                missing.append(name)

            if missing:
                pytest.fail(f"Unknown minion names in WaitWorkflows: {missing}")

        waits: list[asyncio.Future] = []
        for m_cls, count in expected_per_class.items():
            if count <= 0:
                continue
            expected = {step: count for step in m_cls._mn_workflow_spec}  # type: ignore
            waits.append(m_cls.wait_for_calls(expected=expected, timeout=self._timeout))

        if waits:
            await asyncio.gather(*waits)

    def _add_expected_for_receipts(
        self,
        expected_per_class: defaultdict[type[SpiedMinion], int],
        receipts: list[StartReceipt],
    ) -> None:
        for receipt in receipts:
            if not receipt.success:
                continue
            m_cls = receipt.minion_cls or self._spies.minions.get(receipt.minion_modpath)
            if m_cls is None:
                continue
            expected_events = self._plan.pipeline_event_targets.get(receipt.pipeline_modpath)
            if expected_events is None:
                continue
            expected_per_class[m_cls] += expected_events

    async def _wait_minion_tasks(self, minions: set[Minion]) -> None:
        if not minions:
            return

        loop = asyncio.get_running_loop()
        deadline = loop.time() + self._timeout

        while True:
            # todo: consider making Minion._mn_* methods that centralize testing utilitizes that depend on Minion's implementation
            waits: list[asyncio.Future] = []
            pending_tasks: list[asyncio.Task] = []
            for m in minions:
                async with m._mn_tasks_lock:
                    tasks = list(m._mn_tasks | m._mn_aux_tasks)
                if tasks:
                    pending_tasks.extend(tasks)
                    waits.append(asyncio.gather(*tasks, return_exceptions=True))

            if not waits:
                return

            remaining = deadline - loop.time()
            if remaining <= 0:
                names = [
                    task.get_name() if hasattr(task, "get_name") else "task"
                    for task in pending_tasks
                ]
                pytest.fail(
                    "WaitWorkflows timed out while waiting for minion tasks to complete. "
                    f"Pending tasks={len(pending_tasks)} names={names}"
                )

            try:
                await asyncio.wait_for(asyncio.gather(*waits), timeout=remaining)
            except asyncio.TimeoutError:
                names = [
                    task.get_name() if hasattr(task, "get_name") else "task"
                    for task in pending_tasks
                ]
                pytest.fail(
                    "WaitWorkflows timed out while waiting for minion tasks to complete. "
                    f"Pending tasks={len(pending_tasks)} names={names}"
                )
