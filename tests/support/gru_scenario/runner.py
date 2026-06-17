import asyncio
from collections import defaultdict
from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Any, Literal, cast

import pytest

from minions._internal._domain.gru import Gru, GruRuntimeStateSnapshot
from minions._internal._domain.minion import Minion
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.minion_workflow_context_codec import (
    deserialize_workflow_context_blob,
)
from minions._internal._framework.state_store import StateStore
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.mixin_spy import SpyMixin
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource

from .directives import (
    AfterWorkflowStepStarts,
    Concurrent,
    Directive,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    WaitWorkflowCompletions,
    iter_directives_flat,
)
from .introspect import GruIntrospector
from .plan import ScenarioPlan


def _get_minion_event_and_context_types(
    minion_cls: type[Minion[Any, Any]],
) -> tuple[type, type]:
    event_cls = getattr(minion_cls, "_mn_event_cls", None)
    context_cls = getattr(minion_cls, "_mn_workflow_ctx_cls", None)
    if not isinstance(event_cls, type) or not isinstance(context_cls, type):
        raise TypeError(f"{minion_cls.__name__} is missing runtime Minion types.")
    return event_cls, context_cls


# This class name could use a better name,
# assuming it doesn't get replaced after a through audit/review of the gru scenario DSL.
# SpyRegistry seems kind of like a duplicated of the tracking done in Gru.
# So maybe Gru can be used directly to some extent knowing it only has Spied components.
# Have to consider more.
@dataclass
class SpyRegistry:
    minions: dict[str, type[SpiedMinion[Any, Any]]] = field(default_factory=lambda: dict())
    pipelines: dict[str, type[SpiedPipeline[Any]]] = field(default_factory=lambda: dict())
    resources: set[type[SpiedResource]] = field(default_factory=lambda: set())
    resources_by_minion_id: dict[str, frozenset[str]] = field(default_factory=lambda: dict())
    resources_by_pipeline: dict[str, frozenset[str]] = field(default_factory=lambda: dict())
    resource_dependencies_by_dependent_resource: dict[str, frozenset[str]] = field(
        default_factory=lambda: dict()
    )
    pipeline_start_attempt_counts: defaultdict[str, int] = field(
        default_factory=lambda: defaultdict(int)
    )

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
class OrchestrationStartReceipt:
    directive_index: int
    minion_module_path: str
    pipeline_module_path: str
    instance_id: str | None
    minion_cls: type[SpiedMinion[Any, Any]] | None
    success: bool
    orchestration_id: str | None = None
    pipeline_id: str = ""
    minion_id: str = ""

    def __post_init__(self) -> None:
        if not self.pipeline_id:
            raise ValueError("OrchestrationStartReceipt.pipeline_id is required.")
        if not self.minion_id:
            raise ValueError("OrchestrationStartReceipt.minion_id is required.")


@dataclass(frozen=True)
class ScenarioCheckpoint:
    order: int
    kind: str
    directive_type: str
    receipt_count: int
    successful_receipt_count: int
    seen_shutdown: bool
    orchestration_directive_indexes: tuple[int, ...] | None = None
    workflow_steps_mode: Literal["at_least", "exact"] | None = None
    expected_step_starts: dict[int, dict[str, int]] | None = None
    wrapped_directive_type: str | None = None
    spy_call_counts: dict[str, dict[str, int]] | None = None
    spy_call_counts_by_instance: dict[str, dict[int, dict[str, int]]] | None = None
    workflow_step_started_ids_by_minion_id: dict[str, dict[str, tuple[str, ...]]] | None = None
    workflow_step_started_ids_by_orchestration_id: dict[str, dict[str, tuple[str, ...]]] | None = (
        None
    )
    workflow_step_start_events_by_minion_id: (
        dict[str, dict[str, tuple[tuple[int, str], ...]]] | None
    ) = None
    persisted_contexts_by_minion_id: dict[str, int] | None = None
    persisted_contexts_by_orchestration_id: dict[str, int] | None = None
    persisted_context_snapshots_by_minion_id: (
        dict[str, tuple[MinionWorkflowContext[Any, Any], ...]] | None
    ) = None
    metrics_counters: dict[str, list[dict[str, object]]] | None = None


@dataclass(frozen=True)
class LifecycleObservation:
    directive_type: type[Directive]
    receipt_count: int
    active_orchestration_start_indexes: frozenset[int]
    seen_shutdown: bool
    gru_runtime_state: GruRuntimeStateSnapshot


@dataclass
class ScenarioRunResult:
    seen_shutdown: bool = False
    spies: SpyRegistry | None = None
    started_minions: set[SpiedMinion[Any, Any]] = field(default_factory=lambda: set())
    instance_tags: defaultdict[type[SpyMixin], set[int]] = field(
        default_factory=lambda: defaultdict(set)
    )
    extra_calls: list[tuple[type[SpyMixin], tuple[object, ...], dict[str, object]]] = field(
        default_factory=lambda: list()
    )
    receipts: list[OrchestrationStartReceipt] = field(default_factory=lambda: list())
    checkpoints: list[ScenarioCheckpoint] = field(default_factory=lambda: list())
    lifecycle_observations: list[LifecycleObservation] = field(
        default_factory=lambda: list()
    )


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
        self._orchestration_start_receipts_by_directive_id: dict[
            int, OrchestrationStartReceipt
        ] = {}
        self._active_orchestration_start_indexes: set[int] = set()

    async def run(self) -> ScenarioRunResult:
        self._discover_spies()
        self._validate_pipeline_event_targets()
        self._spies.enable_and_reset()
        self._result = ScenarioRunResult()
        self._result.spies = self._spies
        for d in self._plan.directives:
            await self._execute(d)
            if self._contains_lifecycle_command(d):
                self._record_lifecycle_observation(d)
        return self._result

    # should probably make the idea of "lifecycle directive" official
    # and express that a quality of Directive in some way
    def _contains_lifecycle_command(self, d: Directive) -> bool:
        if isinstance(d, (OrchestrationStart, OrchestrationStop, GruShutdown)):
            return True
        if isinstance(d, Concurrent):
            return any(self._contains_lifecycle_command(child) for child in d.directives)
        if isinstance(d, AfterWorkflowStepStarts):
            return self._contains_lifecycle_command(d.directive)
        return False

    def _discover_spies(self) -> None:
        for d in iter_directives_flat(self._plan.directives):
            if not isinstance(d, OrchestrationStart):
                continue

            minion_module_path = d.minion_module_path
            pipeline_module_path = d.pipeline_module_path

            m_cls = (
                d.minion
                if isinstance(d.minion, type)
                else self._insp.get_minion_class(minion_module_path)
            )
            minion_id = self._insp.get_component_identity(m_cls, minion_module_path)
            if minion_id not in self._spies.minions:
                if not issubclass(m_cls, SpiedMinion):
                    pytest.fail(
                        "OrchestrationStart minion must resolve to a spy-enabled minion subclass; "
                        f"got {m_cls!r} for '{minion_module_path}'"
                    )
                self._spies.minions[minion_id] = m_cls

                resources: set[str] = set()
                for r_cls in self._insp.get_all_resource_dependencies(m_cls):
                    if not issubclass(r_cls, SpiedResource):
                        pytest.fail(
                            "Minion resource dependency must be spy-enabled; "
                            f"got {r_cls!r} while resolving '{minion_module_path}'"
                        )
                    self._spies.resources.add(r_cls)
                    resource_id = self._insp.get_resource_identity(r_cls)
                    self._spies.resource_dependencies_by_dependent_resource[resource_id] = (
                        frozenset(
                            self._insp.get_resource_identity(dependency_cls)
                            for dependency_cls in (
                                self._insp.get_direct_resource_dependencies(r_cls)
                            )
                        )
                    )
                resources.update(
                    self._insp.get_resource_identity(r_cls)
                    for r_cls in self._insp.get_direct_resource_dependencies(m_cls)
                )
                self._spies.resources_by_minion_id[minion_id] = frozenset(resources)

            p_cls = (
                d.pipeline
                if isinstance(d.pipeline, type)
                else self._insp.get_pipeline_class(pipeline_module_path)
            )
            pipeline_id = self._insp.get_component_identity(p_cls, pipeline_module_path)
            self._spies.pipeline_start_attempt_counts[pipeline_id] += 1

            if pipeline_id not in self._spies.pipelines:
                if not issubclass(p_cls, SpiedPipeline):
                    pytest.fail(
                        "OrchestrationStart pipeline must resolve to a spy-enabled "
                        f"pipeline subclass; got {p_cls!r} for '{pipeline_module_path}'"
                    )
                self._spies.pipelines[pipeline_id] = p_cls

                resources = set()
                for r_cls in self._insp.get_all_resource_dependencies(p_cls):
                    if not issubclass(r_cls, SpiedResource):
                        pytest.fail(
                            "Pipeline resource dependency must be spy-enabled; "
                            f"got {r_cls!r} while resolving '{pipeline_module_path}'"
                        )
                    self._spies.resources.add(r_cls)
                    resource_id = self._insp.get_resource_identity(r_cls)
                    self._spies.resource_dependencies_by_dependent_resource[resource_id] = (
                        frozenset(
                            self._insp.get_resource_identity(dependency_cls)
                            for dependency_cls in (
                                self._insp.get_direct_resource_dependencies(r_cls)
                            )
                        )
                    )
                resources.update(
                    self._insp.get_resource_identity(r_cls)
                    for r_cls in self._insp.get_direct_resource_dependencies(p_cls)
                )
                self._spies.resources_by_pipeline[pipeline_id] = frozenset(resources)

    def _validate_pipeline_event_targets(self) -> None:
        expected_success_pipelines: set[str] = set()
        for d in iter_directives_flat(self._plan.directives):
            if not isinstance(d, OrchestrationStart) or not d.expect_success:
                continue
            pipeline_module_path = d.pipeline_module_path
            p_cls = (
                d.pipeline
                if isinstance(d.pipeline, type)
                else self._insp.get_pipeline_class(pipeline_module_path)
            )
            expected_success_pipelines.add(
                self._insp.get_component_identity(p_cls, pipeline_module_path)
            )

        configured_pipelines = set(self._plan.pipeline_event_targets)
        missing = sorted(expected_success_pipelines - configured_pipelines)
        if missing:
            pytest.fail(
                "Missing pipeline_event_counts entries for started pipelines: " + ", ".join(missing)
            )

        unused = sorted(configured_pipelines - expected_success_pipelines)
        if unused:
            pytest.fail(
                "pipeline_event_counts contains entries for pipelines not started in directives: "
                + ", ".join(unused)
            )

    async def _execute(self, d: Directive) -> None:
        if isinstance(d, Concurrent):
            await asyncio.gather(*[self._execute(child) for child in d.directives])

        elif isinstance(d, OrchestrationStart):
            await self._run_start(d)

        elif isinstance(d, OrchestrationStop):
            await self._run_stop(d)

        elif isinstance(d, WaitWorkflowCompletions):
            await self._wait_workflows(d)

        elif isinstance(d, AfterWorkflowStepStarts):
            await self._wait_workflow_step_starts_then(d)

        elif isinstance(d, ExpectRuntime):
            await self._run_expect_runtime(d)

        elif isinstance(d, GruShutdown):
            await self._run_shutdown(d)

        else:
            pytest.fail(f"Unknown directive: {d!r}")

    async def _run_start(self, d: OrchestrationStart) -> None:
        result = self._require_result()
        directive_index = self._plan.directive_index(d)
        if isinstance(d.minion, str):
            if not isinstance(d.pipeline, str):
                pytest.fail(
                    "OrchestrationStart.pipeline must be a module path string when "
                    "OrchestrationStart.minion is a module path string."
                )
            if d.minion_config is not None:
                pytest.fail(
                    "OrchestrationStart.minion_config is only supported for class-based starts; "
                    "use minion_config_path with module path strings."
                )
            r = await self._gru.start_orchestration(
                pipeline=d.pipeline,
                minion=d.minion,
                minion_config_path=d.minion_config_path,
            )
        else:
            if isinstance(d.pipeline, str):
                pytest.fail(
                    "OrchestrationStart.pipeline must be a Pipeline subclass when "
                    "OrchestrationStart.minion is a Minion subclass."
                )
            if d.minion_config_path is not None:
                pytest.fail(
                    "OrchestrationStart.minion_config_path is only supported for "
                    "module path starts; use minion_config with class-based starts."
                )
            r = await self._gru.start_orchestration(
                pipeline=d.pipeline,
                minion=d.minion,
                minion_config=d.minion_config,
            )
        if r.success != d.expect_success:
            pytest.fail(f"start_orchestration mismatch: {d} -> {r}")

        receipt = OrchestrationStartReceipt(
            directive_index=directive_index,
            minion_module_path=d.minion_module_path,
            pipeline_module_path=d.pipeline_module_path,
            minion_id=(
                self._insp.get_component_identity(d.minion, d.minion_module_path)
                if isinstance(d.minion, type)
                else self._insp.get_minion_identity_from_module_path(d.minion_module_path)
            ),
            pipeline_id=(
                self._insp.get_component_identity(d.pipeline, d.pipeline_module_path)
                if isinstance(d.pipeline, type)
                else self._insp.get_pipeline_identity_from_module_path(d.pipeline_module_path)
            ),
            instance_id=None,
            minion_cls=None,
            success=r.success,
            orchestration_id=getattr(r, "orchestration_id", None),
        )

        if not r.success:
            result.receipts.append(receipt)
            self._orchestration_start_receipts_by_directive_id[id(d)] = receipt
            return

        minion_inst = None
        if receipt.orchestration_id:
            minion_inst = self._insp.get_minion_by_orchestration_id(receipt.orchestration_id)

        if minion_inst is not None:
            m_cls = type(minion_inst)
            minion_cls = m_cls if issubclass(m_cls, SpiedMinion) else None
            receipt = OrchestrationStartReceipt(
                directive_index=receipt.directive_index,
                minion_module_path=receipt.minion_module_path,
                pipeline_module_path=receipt.pipeline_module_path,
                minion_id=getattr(minion_inst, "_mn_minion_id", receipt.minion_id),
                pipeline_id=receipt.pipeline_id,
                instance_id=getattr(minion_inst, "_mn_minion_instance_id", None),
                minion_cls=minion_cls,
                success=receipt.success,
                orchestration_id=receipt.orchestration_id,
            )
            if isinstance(minion_inst, SpiedMinion):
                result.started_minions.add(minion_inst)

        result.receipts.append(receipt)
        self._orchestration_start_receipts_by_directive_id[id(d)] = receipt
        self._active_orchestration_start_indexes.add(receipt.directive_index)
        self._record_instance_tags(minion_inst, receipt.pipeline_id, receipt.instance_id)

    async def _run_stop(self, d: OrchestrationStop) -> None:
        target_id = self._resolve_stop_target_id(d.id)
        if target_id is None:
            pytest.fail(f"stop target could not be resolved from {d.id!r}")
        r = await self._gru.stop_orchestration(orchestration_id=target_id)
        if r.success != d.expect_success:
            pytest.fail(f"stop_orchestration mismatch: {d} -> {r}")
        if r.success:
            stopped_receipt = self._resolve_stop_target_receipt(d.id, target_id)
            if stopped_receipt is not None:
                self._active_orchestration_start_indexes.discard(
                    stopped_receipt.directive_index
                )

    def _resolve_stop_target_id(self, target: str | OrchestrationStart) -> str | None:
        if isinstance(target, str):
            return target

        receipt = self._orchestration_start_receipts_by_directive_id.get(id(target))
        if receipt is None or not receipt.success or receipt.orchestration_id is None:
            return None
        return receipt.orchestration_id

    def _resolve_stop_target_receipt(
        self,
        target: str | OrchestrationStart,
        target_id: str,
    ) -> OrchestrationStartReceipt | None:
        if isinstance(target, OrchestrationStart):
            return self._orchestration_start_receipts_by_directive_id.get(id(target))
        return next(
            (
                receipt
                for receipt in reversed(self._require_result().receipts)
                if (
                    receipt.success
                    and receipt.orchestration_id == target_id
                    and receipt.directive_index in self._active_orchestration_start_indexes
                )
            ),
            None,
        )

    async def _run_shutdown(self, d: GruShutdown) -> None:
        result = self._require_result()
        r = await self._gru.shutdown()
        if r.success != d.expect_success:
            pytest.fail(f"shutdown mismatch: expected {d.expect_success}, got {r}")
        result.seen_shutdown = r.success
        if r.success:
            self._active_orchestration_start_indexes.clear()
        await self._record_checkpoint(kind="gru_shutdown", directive=d)

    async def _wait_workflows(self, d: WaitWorkflowCompletions) -> None:
        waiter = ScenarioWaiter(
            self._plan,
            self._insp,
            self._timeout,
            self._spies,
            result=self._require_result(),
        )
        await waiter.wait(orchestrations=d.orchestrations)
        await self._record_checkpoint(
            kind="wait_workflow_completions",
            directive=d,
            orchestrations=d.orchestrations,
            workflow_steps_mode=d.workflow_steps_mode,
        )

    async def _wait_workflow_step_starts_then(self, d: AfterWorkflowStepStarts) -> None:
        if not isinstance(d.directive, OrchestrationStop):
            pytest.fail(
                "AfterWorkflowStepStarts currently supports wrapping OrchestrationStop only; "
                f"got {type(d.directive).__name__}"
            )
        waiter = ScenarioWaiter(
            self._plan,
            self._insp,
            self._timeout,
            self._spies,
            result=self._require_result(),
        )
        await waiter.wait_for_step_starts(expected=d.expected)
        await self._run_stop(d.directive)
        await self._record_checkpoint(
            kind="wait_workflow_step_starts_then",
            directive=d,
            expected_step_starts=d.expected,
            wrapped_directive_type=type(d.directive).__name__,
        )

    async def _run_expect_runtime(self, d: ExpectRuntime) -> None:
        await self._record_checkpoint(
            kind="expect_runtime",
            directive=d,
        )

    def _record_instance_tags(
        self,
        minion_inst: Minion[Any, Any] | None,
        pipeline_id: str,
        instance_id: str | None,
    ) -> None:
        self._record_tag_if_present(minion_inst)
        if instance_id is None:
            return

        pipeline_inst = self._insp.get_pipeline_instance(pipeline_id)
        self._record_tag_if_present(pipeline_inst)

        resource_ids = self._insp.resource_ids_for(
            minion_instance_id=instance_id,
            pipeline_id=pipeline_id,
        )
        for rid in resource_ids:
            res_inst = self._insp.get_resource_instance(rid)
            self._record_tag_if_present(res_inst)

    def _record_tag_if_present(self, inst: object | None) -> None:
        if inst is None:
            return
        if not isinstance(inst, SpyMixin):
            return
        tag = getattr(inst, "_mspy_instance_tag", None)
        if tag is None:
            return
        result = self._require_result()
        result.instance_tags[type(inst)].add(tag)

    def _require_result(self) -> ScenarioRunResult:
        if self._result is None:
            raise AssertionError(
                "ScenarioRunner internal invariant violated: _result is None. "
                "Call and await ScenarioRunner.run() before invoking directive execution "
                "or checkpoint/snapshot helpers."
            )
        return self._result

    async def _record_checkpoint(
        self,
        *,
        kind: str,
        directive: Directive,
        orchestrations: tuple[OrchestrationStart, ...] | None = None,
        workflow_steps_mode: Literal["at_least", "exact"] | None = None,
        expected_step_starts: dict[OrchestrationStart, dict[str, int]] | None = None,
        wrapped_directive_type: str | None = None,
    ) -> None:
        result = self._require_result()
        persisted_context_snapshots = (
            await self._snapshot_persisted_context_snapshots_by_minion_id()
        )
        checkpoint = ScenarioCheckpoint(
            order=len(result.checkpoints),
            kind=kind,
            directive_type=type(directive).__name__,
            receipt_count=len(result.receipts),
            successful_receipt_count=sum(1 for r in result.receipts if r.success),
            seen_shutdown=result.seen_shutdown,
            orchestration_directive_indexes=(
                tuple(self._plan.directive_index(start) for start in orchestrations)
                if orchestrations is not None
                else None
            ),
            workflow_steps_mode=workflow_steps_mode,
            expected_step_starts={
                self._plan.directive_index(start): dict(steps)
                for start, steps in expected_step_starts.items()
            }
            if expected_step_starts is not None
            else None,
            wrapped_directive_type=wrapped_directive_type,
            spy_call_counts=self._snapshot_spy_call_counts(),
            spy_call_counts_by_instance=self._snapshot_spy_call_counts_by_instance(),
            workflow_step_started_ids_by_minion_id=self._snapshot_workflow_step_started_ids_by_minion_id(),
            workflow_step_started_ids_by_orchestration_id=(
                self._snapshot_workflow_step_started_ids_by_orchestration_id()
            ),
            workflow_step_start_events_by_minion_id=self._snapshot_workflow_step_start_events_by_minion_id(),
            persisted_contexts_by_minion_id=self._count_persisted_context_snapshots(
                persisted_context_snapshots,
            ),
            persisted_contexts_by_orchestration_id=(
                self._count_persisted_context_snapshots_by_orchestration_id(
                    persisted_context_snapshots,
                )
            ),
            persisted_context_snapshots_by_minion_id=persisted_context_snapshots,
            metrics_counters=self._snapshot_metrics_counters(),
        )
        result.checkpoints.append(checkpoint)

    def _record_lifecycle_observation(self, directive: Directive) -> None:
        result = self._require_result()
        result.lifecycle_observations.append(
            LifecycleObservation(
                directive_type=type(directive),
                receipt_count=len(result.receipts),
                active_orchestration_start_indexes=frozenset(
                    self._active_orchestration_start_indexes
                ),
                seen_shutdown=result.seen_shutdown,
                gru_runtime_state=self._insp.runtime_state_snapshot(),
            )
        )

    def _snapshot_spy_call_counts(self) -> dict[str, dict[str, int]]:
        classes: set[type[SpyMixin]] = set()
        classes.update(self._spies.minions.values())
        classes.update(self._spies.pipelines.values())
        classes.update(self._spies.resources)

        state_store = getattr(self._gru, "_state_store", None)
        if isinstance(state_store, SpyMixin):
            classes.add(type(state_store))

        snapshots: dict[str, dict[str, int]] = {}
        for cls in classes:
            key = f"{cls.__module__}.{cls.__name__}"
            snapshots[key] = cls.get_call_counts()
        return snapshots

    def _snapshot_spy_call_counts_by_instance(self) -> dict[str, dict[int, dict[str, int]]]:
        classes: set[type[SpyMixin]] = set()
        classes.update(self._spies.minions.values())
        classes.update(self._spies.pipelines.values())
        classes.update(self._spies.resources)

        state_store = getattr(self._gru, "_state_store", None)
        if isinstance(state_store, SpyMixin):
            classes.add(type(state_store))

        snapshots: dict[str, dict[int, dict[str, int]]] = {}
        for cls in classes:
            by_tag: defaultdict[int, defaultdict[str, int]] = defaultdict(lambda: defaultdict(int))
            for name, _, tag in cls.get_call_history():
                if tag is None:
                    continue
                by_tag[tag][name] += 1

            key = f"{cls.__module__}.{cls.__name__}"
            snapshots[key] = {tag: dict(counts) for tag, counts in by_tag.items()}
        return snapshots

    def _snapshot_workflow_step_started_ids_by_minion_id(
        self,
    ) -> dict[str, dict[str, tuple[str, ...]]] | None:
        logger = getattr(self._gru, "_logger", None)
        logs = getattr(logger, "logs", None)
        if not isinstance(logs, list):
            return None
        logs = cast(list[object], logs)

        by_minion_id_step: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )

        for log in logs:
            if getattr(log, "msg", None) != "Workflow Step started":
                continue
            kwargs = getattr(log, "kwargs", None)
            if not isinstance(kwargs, dict):
                continue
            kwargs = cast(dict[str, object], kwargs)

            minion_id = kwargs.get("minion_id")
            step_name = kwargs.get("step_name")
            workflow_id = kwargs.get("workflow_id")
            if (
                not isinstance(minion_id, str)
                or not isinstance(step_name, str)
                or not isinstance(workflow_id, str)
            ):
                continue

            by_minion_id_step[minion_id][step_name].add(workflow_id)

        return {
            minion_id: {
                step_name: tuple(sorted(workflow_ids))
                for step_name, workflow_ids in by_step.items()
            }
            for minion_id, by_step in by_minion_id_step.items()
        }

    def _snapshot_workflow_step_started_ids_by_orchestration_id(
        self,
    ) -> dict[str, dict[str, tuple[str, ...]]]:
        logger = self._gru._logger

        assert isinstance(logger, InMemoryLogger), "InMemoryLogger required"

        by_orchestration_step: defaultdict[str, defaultdict[str, set[str]]] = defaultdict(
            lambda: defaultdict(set)
        )
        for log in logger.logs:
            if log.msg != "Workflow Step started":
                continue
            orchestration_id = log.kwargs.get("orchestration_id")
            step_name = log.kwargs.get("step_name")
            workflow_id = log.kwargs.get("workflow_id")
            if (
                not isinstance(orchestration_id, str)
                or not isinstance(step_name, str)
                or not isinstance(workflow_id, str)
            ):
                continue
            by_orchestration_step[orchestration_id][step_name].add(workflow_id)

        return {
            orchestration_id: {
                step_name: tuple(sorted(workflow_ids))
                for step_name, workflow_ids in by_step.items()
            }
            for orchestration_id, by_step in by_orchestration_step.items()
        }

    def _snapshot_workflow_step_start_events_by_minion_id(
        self,
    ) -> dict[str, dict[str, tuple[tuple[int, str], ...]]] | None:
        logger = getattr(self._gru, "_logger", None)
        logs = getattr(logger, "logs", None)
        if not isinstance(logs, list):
            return None
        logs = cast(list[object], logs)

        by_minion_id_workflow: defaultdict[str, defaultdict[str, list[tuple[int, str]]]] = (
            defaultdict(lambda: defaultdict(list))
        )

        for log in logs:
            if getattr(log, "msg", None) != "Workflow Step started":
                continue
            kwargs = getattr(log, "kwargs", None)
            if not isinstance(kwargs, dict):
                continue
            kwargs = cast(dict[str, object], kwargs)

            minion_id = kwargs.get("minion_id")
            step_name = kwargs.get("step_name")
            step_index = kwargs.get("step_index")
            workflow_id = kwargs.get("workflow_id")
            if (
                not isinstance(minion_id, str)
                or not isinstance(step_name, str)
                or not isinstance(step_index, int)
                or not isinstance(workflow_id, str)
            ):
                continue

            by_minion_id_workflow[minion_id][workflow_id].append((step_index, step_name))

        return {
            minion_id: {workflow_id: tuple(events) for workflow_id, events in by_workflow.items()}
            for minion_id, by_workflow in by_minion_id_workflow.items()
        }

    async def _snapshot_persisted_context_snapshots_by_minion_id(
        self,
    ) -> dict[str, tuple[MinionWorkflowContext[Any, Any], ...]] | None:
        state_store = getattr(self._gru, "_state_store", None)
        if not isinstance(state_store, StateStore):
            return None
        try:
            stored_contexts = await state_store._mn_get_all_contexts()
        except Exception:
            return None

        receipts_by_orchestration_id = {
            r.orchestration_id: r
            for r in self._require_result().receipts
            if r.success and r.orchestration_id
        }
        contexts_by_minion_id: defaultdict[str, list[MinionWorkflowContext[Any, Any]]] = (
            defaultdict(list)
        )
        try:
            for stored_context in stored_contexts:
                receipt = receipts_by_orchestration_id.get(stored_context.orchestration_id)
                ctx: MinionWorkflowContext[Any, Any]
                if receipt is not None and receipt.minion_cls is not None:
                    event_cls, context_cls = _get_minion_event_and_context_types(receipt.minion_cls)
                    ctx = deserialize_workflow_context_blob(
                        stored_context.context,
                        event_cls=event_cls,
                        context_cls=context_cls,
                    )
                else:
                    ctx = deserialize_workflow_context_blob(stored_context.context)
                if receipt is None:
                    continue
                minion_id = receipt.minion_id
                contexts_by_minion_id[minion_id].append(ctx)
        except Exception:
            return None

        return {
            minion_id: tuple(sorted(contexts, key=lambda ctx: ctx.workflow_id))
            for minion_id, contexts in contexts_by_minion_id.items()
        }

    def _count_persisted_context_snapshots(
        self,
        snapshots: dict[str, tuple[MinionWorkflowContext[Any, Any], ...]] | None,
    ) -> dict[str, int] | None:
        if snapshots is None:
            return None
        counts: defaultdict[str, int] = defaultdict(int)
        for minion_id, contexts in snapshots.items():
            counts[minion_id] += len(contexts)
        return dict(counts)

    def _count_persisted_context_snapshots_by_orchestration_id(
        self,
        snapshots: dict[str, tuple[MinionWorkflowContext[Any, Any], ...]] | None,
    ) -> dict[str, int] | None:
        if snapshots is None:
            return None
        counts: defaultdict[str, int] = defaultdict(int)
        for contexts in snapshots.values():
            for ctx in contexts:
                counts[ctx.orchestration_id] += 1
        return dict(counts)

    def _snapshot_metrics_counters(self) -> dict[str, list[dict[str, object]]] | None:
        metrics = getattr(self._gru, "_metrics", None)
        # TODO: should we call the user-defined method directly or via a wrapper
        # like _mn_snapshot (or create _mn_snapshot_counters if needed)?
        snapshot_fn = getattr(metrics, "snapshot_counters", None)

        if not callable(snapshot_fn):
            return None

        counters = snapshot_fn()

        if not isinstance(counters, dict):
            return None

        counters = cast(dict[str, object], counters)

        normalized: dict[str, list[dict[str, object]]] = {}

        for name, samples in counters.items():
            if not isinstance(samples, list):
                normalized[name] = []
                continue

            samples = cast(list[object], samples)
            normalized_samples: list[dict[str, object]] = []

            for sample in samples:
                if not isinstance(sample, dict):
                    continue

                sample = cast(dict[str, object], sample)
                normalized_samples.append(sample)

            normalized[name] = normalized_samples

        return normalized


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

    async def wait(self, *, orchestrations: tuple[OrchestrationStart, ...] | None) -> None:
        await self._wait_expected_workflow_calls(orchestrations=orchestrations)
        await self._wait_minion_tasks(self._result.started_minions)

    async def wait_for_step_starts(
        self,
        *,
        expected: dict[OrchestrationStart, dict[str, int]],
    ) -> None:
        if not expected:
            pytest.fail("AfterWorkflowStepStarts.expected must be a non-empty dict.")
        waits: list[Awaitable[None]] = []
        for start, steps in expected.items():
            if not steps:
                pytest.fail("AfterWorkflowStepStarts.expected values must be non-empty dicts.")

            directive_index = self._plan.directive_index(start)
            receipt = next(
                (r for r in self._result.receipts if r.directive_index == directive_index),
                None,
            )
            if receipt is None:
                pytest.fail(
                    "AfterWorkflowStepStarts references a start that has not executed: "
                    f"{directive_index}"
                )

            m_cls = receipt.minion_cls or self._spies.minions.get(receipt.minion_id)
            if m_cls is None:
                continue
            workflow = tuple(m_cls._mn_workflow_spec or ())
            for step_name, count in steps.items():
                if count <= 0:
                    pytest.fail(
                        f"AfterWorkflowStepStarts.expected[{step_name!r}] "
                        f"must be >= 1, got {count}."
                    )
                if step_name not in workflow:
                    pytest.fail(
                        f"Unknown workflow step in AfterWorkflowStepStarts.expected: {step_name}"
                    )
                waits.append(m_cls.wait_for_call(step_name, count=count, timeout=self._timeout))

        if waits:
            await asyncio.gather(*waits)

    async def _wait_expected_workflow_calls(
        self,
        *,
        orchestrations: tuple[OrchestrationStart, ...] | None,
    ) -> None:
        if orchestrations is not None and not orchestrations:
            return

        expected_per_class: defaultdict[type[SpiedMinion[Any, Any]], int] = defaultdict(int)

        if orchestrations is None:
            self._add_expected_for_receipts(expected_per_class, self._result.receipts)
        else:
            receipts: list[OrchestrationStartReceipt] = []
            missing: list[int] = []
            for start in orchestrations:
                receipt = next(
                    (
                        item
                        for item in self._result.receipts
                        if item.directive_index == self._plan.directive_index(start)
                    ),
                    None,
                )
                if receipt is None:
                    missing.append(self._plan.directive_index(start))
                    continue
                receipts.append(receipt)

            if missing:
                pytest.fail(
                    f"WaitWorkflowCompletions references starts that have not executed: {missing}"
                )
            self._add_expected_for_receipts(expected_per_class, receipts)

        waits: list[Awaitable[None]] = []
        for m_cls, count in expected_per_class.items():
            if count <= 0:
                continue
            expected = {step: count for step in m_cls._mn_workflow_spec}  # type: ignore
            waits.append(m_cls.wait_for_calls(expected=expected, timeout=self._timeout))

        if waits:
            await asyncio.gather(*waits)

    def _add_expected_for_receipts(
        self,
        expected_per_class: defaultdict[type[SpiedMinion[Any, Any]], int],
        receipts: list[OrchestrationStartReceipt],
    ) -> None:
        for receipt in receipts:
            if not receipt.success:
                continue
            m_cls = receipt.minion_cls or self._spies.minions.get(receipt.minion_id)
            if m_cls is None:
                continue
            expected_events = self._plan.pipeline_event_targets.get(receipt.pipeline_id)
            if expected_events is None:
                continue
            expected_per_class[m_cls] += expected_events

    async def _wait_minion_tasks(self, minions: set[SpiedMinion[Any, Any]]) -> None:
        if not minions:
            return

        minions_tup = tuple(minions)
        results = await asyncio.gather(
            *(m._mn_wait_until_all_tasks_idle(timeout=self._timeout) for m in minions_tup),
            return_exceptions=True,
        )

        failure_messages: list[str] = []
        for m, result in zip(minions_tup, results, strict=True):
            if isinstance(result, TimeoutError):
                async with m._mn_tasks_gate:
                    pending_tasks = list(m._mn_workflow_tasks | m._mn_service_tasks)
                names = [
                    task.get_name() if hasattr(task, "get_name") else "task"
                    for task in pending_tasks
                ]
                failure_messages.append(
                    "WaitWorkflowCompletions timed out while waiting for minion tasks to complete. "
                    f"Pending tasks={len(pending_tasks)} names={names}"
                )
            elif isinstance(result, BaseException):
                raise result

        if failure_messages:
            pytest.fail("\n".join(failure_messages))
