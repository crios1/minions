import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Any, Callable, cast

import pytest

from minions._internal._framework.metrics_constants import (
    LABEL_ORCHESTRATION_ID,
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
)
from tests.assets.support.logger_spied import SpiedLogger
from tests.assets.support.metrics_spied import SpiedMetrics
from tests.assets.support.minion_spied import SpiedMinion
from tests.assets.support.mixin_spy import SpyMixin
from tests.assets.support.pipeline_spied import SpiedPipeline
from tests.assets.support.resource_spied import SpiedResource
from tests.assets.support.state_store_spied import SpiedStateStore

from .directives import ExpectRuntime, OrchestrationStart
from .plan import ScenarioPlan
from .runner import (
    OrchestrationStartReceipt,
    ScenarioCheckpoint,
    ScenarioRunResult,
    SpyRegistry,
    _get_minion_event_and_context_types,
)


class _ExtraCallRecorder:
    def __init__(
        self,
        result: ScenarioRunResult,
        cls: type[SpiedMinion[Any, Any]] | type[SpiedResource] | type[SpiedStateStore],
    ):
        self._result = result
        self._cls = cls

    def __call__(self, *a: object, **kw: object) -> None:
        self._result.extra_calls.append((self._cls, a, kw))


def _names_for_tag(cls: type[SpyMixin], tag: int) -> list[str]:
    return [n for n, _, t in cls.get_call_history() if t == tag]


@dataclass(frozen=True)
class MinionExpectations:
    """Computed expectations for minion starts and workflow calls."""

    minion_start_counts: defaultdict[type[SpiedMinion[Any, Any]], int]
    expected_workflows_by_class: defaultdict[type[SpiedMinion[Any, Any]], int]


@dataclass(frozen=True)
class ExpectedCallCounts:
    """Expected call counts plus spy classes allowed to have extra calls."""

    call_counts: dict[
        type[SpiedMinion[Any, Any]] | type[SpiedResource] | type[SpiedStateStore],
        dict[str, int],
    ]
    allow_unlisted: set[type[SpiedPipeline[Any]] | type[SpiedResource]]


@dataclass(frozen=True)
class PipelineAttemptOutcomes:
    attempts_by_pipeline_id: defaultdict[str, int]
    successes_by_pipeline_id: defaultdict[str, int]


@dataclass(frozen=True)
class WindowStepProgressionCheck:
    call_expected_min: int
    call_expected_max: int
    call_actual: int
    workflow_id_expected_min: int
    workflow_id_expected_max: int
    workflow_id_actual: int
    has_workflow_id_evidence: bool
    workflow_id_mismatch: bool
    call_mismatch: bool


class ScenarioVerifier:
    def __init__(
        self,
        plan: ScenarioPlan,
        result: ScenarioRunResult,
        *,
        logger: SpiedLogger,
        metrics: SpiedMetrics,
        state_store: SpiedStateStore,
        per_verification_timeout: float,
    ):
        self._plan = plan
        self._result = result
        self._logger = logger
        self._metrics = metrics
        self._state_store = state_store
        self._timeout = per_verification_timeout

    @staticmethod
    def _require_workflow_spec(
        m_cls: type[SpiedMinion[Any, Any]],
    ) -> tuple[str, ...]:
        workflow = m_cls._mn_workflow_spec
        if workflow is None:
            pytest.fail(
                f"{m_cls.__name__}: workflow spec missing; "
                "Minion setup is deferred or incomplete."
            )
        if not workflow:
            pytest.fail(
                f"{m_cls.__name__}: workflow spec is empty; "
                "a valid Minion must define at least one @minion_step."
            )
        return workflow

    async def verify(self) -> None:
        expected = self._build_expected_call_counts()
        self._assert_metrics_label_contract()
        self._assert_runtime_expectations()
        self._assert_lifecycle_tracking()
        self._assert_persisted_context_integrity()
        self._assert_pipeline_events()
        self._assert_workflow_step_start_events_are_monotonic()
        self._assert_checkpoint_window_workflow_step_progression()

        unpin_fns: list[Callable[[], None]] = []
        try:
            unpin_fns = await self._pin_and_assert_calls(
                expected.call_counts, expected.allow_unlisted
            )
            self._assert_state_store_read_call_bounds()
            self._assert_minion_fanout_delivery()
            self._assert_call_order(expected.call_counts)
        finally:
            for unpin in unpin_fns:
                unpin()

    def _assert_metrics_label_contract(self) -> None:
        assert_fn = getattr(self._metrics, "assert_recorded_labels_match_contract", None)
        if not callable(assert_fn):
            return
        try:
            assert_fn()
        except AssertionError as e:
            pytest.fail(f"Metrics label contract mismatch: {e}")

    def _build_expected_call_counts(self) -> ExpectedCallCounts:
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)
        call_counts: dict[
            type[SpiedMinion[Any, Any]] | type[SpiedResource] | type[SpiedStateStore],
            dict[str, int],
        ] = {}
        allow_unlisted: set[type[SpiedPipeline[Any]] | type[SpiedResource]] = set()

        for r_cls in spies.resources:
            call_counts[r_cls] = {"__init__": 1, "startup": 1, "run": 1}
            allow_unlisted.add(r_cls)

        for p_cls in spies.pipelines.values():
            # TODO: Decide whether pipelines should be pinned in _pin_and_assert_calls.
            # This allow-list entry is currently inert because pipelines are not added
            # to call_counts; pipeline behavior is asserted separately in
            # _assert_pipeline_events().
            allow_unlisted.add(p_cls)

        for m_cls in spies.minions.values():
            starts = expectations.minion_start_counts.get(m_cls, 0)
            replayed_step_counts = self._compute_replayed_step_counts(spies).get(m_cls, {})
            workflow = self._require_workflow_spec(m_cls)
            base = {
                "__init__": starts,
                "startup": starts,
                "run": starts,
                **{
                    name: (
                        expectations.expected_workflows_by_class.get(m_cls, 0)
                        + replayed_step_counts.get(name, 0)
                    )
                    for name in workflow
                },
            }
            call_counts[m_cls] = base

        minion_starts = sum(expectations.minion_start_counts.values())
        workflows_started = sum(expectations.expected_workflows_by_class.values())
        unresolved_workflows = self._count_unresolved_persisted_workflows(spies)
        resolved_workflows = max(workflows_started - unresolved_workflows, 0)
        replayed_step_counts_by_class = self._compute_replayed_step_counts(spies)
        workflow_steps = 0
        for m in spies.minions.values():
            workflow = self._require_workflow_spec(m)
            workflow_steps += len(workflow) * expectations.expected_workflows_by_class.get(m, 0)
            workflow_steps += sum(replayed_step_counts_by_class.get(m, {}).values())
        total_save_operations = workflows_started + workflow_steps
        checkpoint_reads = len(self._result.checkpoints)
        total_decode_operations = minion_starts

        ss = {
            "__init__": 1,
            "startup": 1,
            "get_contexts_for_orchestration": minion_starts,
            "_mn_get_contexts_for_orchestration": minion_starts,
            "_mn_get_decoded_contexts_for_orchestration": minion_starts,
            "_mn_decode_stored_contexts": total_decode_operations,
            "save_context": total_save_operations,
            "_mn_save_context": total_save_operations,
            "_mn_serialize_and_save_context": total_save_operations,
            "delete_context": resolved_workflows,
            "_mn_delete_context": resolved_workflows,
        }
        if checkpoint_reads > 0:
            ss["get_all_contexts"] = checkpoint_reads
            ss["_mn_get_all_contexts"] = checkpoint_reads
        if self._result.seen_shutdown:
            ss["shutdown"] = 1
        call_counts[type(self._state_store)] = ss

        return ExpectedCallCounts(call_counts=call_counts, allow_unlisted=allow_unlisted)

    def _compute_replayed_step_counts(
        self,
        spies: SpyRegistry,
    ) -> dict[type[SpiedMinion[Any, Any]], dict[str, int]]:
        replayed: defaultdict[type[SpiedMinion[Any, Any]], defaultdict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        seen: set[tuple[str, str]] = set()
        for checkpoint in self._result.checkpoints:
            snapshots = checkpoint.persisted_context_snapshots_by_minion_id
            if not snapshots:
                continue
            later_successful_minion_ids = {
                r.minion_id
                for r in self._result.receipts[checkpoint.receipt_count:]
                if r.success
            }
            for minion_id, contexts in snapshots.items():
                if minion_id not in later_successful_minion_ids:
                    continue
                receipt = next(
                    (
                        r for r in self._result.receipts
                        if r.success and r.minion_id == minion_id
                    ),
                    None,
                )
                m_cls = receipt.minion_cls if receipt is not None else None
                if m_cls is None and receipt is not None:
                    m_cls = spies.minions.get(receipt.minion_id)
                if m_cls is None:
                    continue
                workflow = self._require_workflow_spec(m_cls)
                for ctx in contexts:
                    for step_name in workflow[ctx.next_step_index:]:
                        key = (ctx.workflow_id, step_name)
                        if key in seen:
                            continue
                        seen.add(key)
                        replayed[m_cls][step_name] += 1
        return {
            m_cls: dict(counts)
            for m_cls, counts in replayed.items()
        }

    def _count_unresolved_persisted_workflows(self, spies: SpyRegistry) -> int:
        latest_with_persistence = next(
            (
                cp
                for cp in reversed(self._result.checkpoints)
                if cp.persisted_contexts_by_minion_id is not None
            ),
            None,
        )
        if (
            latest_with_persistence is None
            or latest_with_persistence.persisted_contexts_by_minion_id is None
        ):
            return 0
        persisted = latest_with_persistence.persisted_contexts_by_minion_id
        tracked_minion_ids = {
            r.minion_id
            for r in self._result.receipts
            if r.success and (r.minion_cls is not None or r.minion_id in persisted)
        }
        return sum(
            count for minion_id, count in persisted.items() if minion_id in tracked_minion_ids
        )

    def _assert_persisted_context_integrity(self) -> None:
        spies = self._require_spies()
        receipt_by_orchestration_id = {
            r.orchestration_id: r
            for r in self._result.receipts
            if r.success and r.orchestration_id
        }

        for checkpoint in self._result.checkpoints:
            snapshots = checkpoint.persisted_context_snapshots_by_minion_id
            if snapshots is None:
                continue
            for minion_id, contexts in snapshots.items():
                for ctx in contexts:
                    receipt = receipt_by_orchestration_id.get(ctx.orchestration_id)
                    if receipt is None:
                        pytest.fail(
                            "Persisted context has no matching successful start receipt: "
                            f"workflow_id={ctx.workflow_id!r}, "
                            f"orchestration_id={ctx.orchestration_id!r}, "
                            f"checkpoint={checkpoint.order}."
                        )
                    if receipt.minion_id != minion_id:
                        pytest.fail(
                            "Persisted context snapshot minion_id mismatch: "
                            f"workflow_id={ctx.workflow_id!r}, "
                            f"expected {receipt.minion_id!r}, got {minion_id!r}."
                        )
                    m_cls = receipt.minion_cls or spies.minions.get(receipt.minion_id)
                    if m_cls is None:
                        pytest.fail(
                            "Persisted context snapshot references unknown minion_id "
                            f"{minion_id!r} at checkpoint {checkpoint.order}."
                        )
                    workflow_len = len(self._require_workflow_spec(m_cls))
                    event_cls, context_cls = _get_minion_event_and_context_types(m_cls)
                    if not isinstance(ctx.event, event_cls):
                        pytest.fail(
                            "Persisted context event type mismatch: "
                            f"workflow_id={ctx.workflow_id!r}, "
                            f"expected {event_cls.__name__}, got {type(ctx.event).__name__}."
                        )
                    if not isinstance(ctx.context, context_cls):
                        pytest.fail(
                            "Persisted context context type mismatch: "
                            f"workflow_id={ctx.workflow_id!r}, "
                            f"expected {context_cls.__name__}, got {type(ctx.context).__name__}."
                        )
                    if ctx.context_cls is not context_cls:
                        pytest.fail(
                            "Persisted context context_cls mismatch: "
                            f"workflow_id={ctx.workflow_id!r}, "
                            f"expected {context_cls!r}, got {ctx.context_cls!r}."
                        )
                    if ctx.next_step_index < 0 or ctx.next_step_index >= workflow_len:
                        pytest.fail(
                            "Persisted context next_step_index out of bounds: "
                            f"workflow_id={ctx.workflow_id!r}, "
                            f"next_step_index={ctx.next_step_index}, workflow_len={workflow_len}."
                        )

    def _assert_state_store_read_call_bounds(self) -> None:
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)
        minion_starts = sum(expectations.minion_start_counts.values())
        checkpoint_reads = len(self._result.checkpoints)
        state_store_counts = type(self._state_store).get_call_counts()
        get_all_calls = state_store_counts.get("get_all_contexts", 0)
        allowed_reads = minion_starts + checkpoint_reads
        if get_all_calls > allowed_reads:
            pytest.fail(
                "StateStore.get_all_contexts called more times than allowed "
                "(minion starts + checkpoint snapshots): "
                f"{get_all_calls} > {allowed_reads}"
            )

    def _assert_lifecycle_tracking(self) -> None:
        spies = self._require_spies()
        for observation_index, observation in enumerate(
            self._result.lifecycle_observations
        ):
            expected_by_state: dict[str, object]
            if observation.seen_shutdown:
                expected_by_state = {
                    "minion_instances": frozenset(),
                    "orchestrations": frozenset(),
                    "minion_tasks": frozenset(),
                    "pipelines": frozenset(),
                    "pipeline_tasks": frozenset(),
                    "resources": frozenset(),
                    "resource_tasks": frozenset(),
                    "pipeline_by_minion_instance": {},
                    "resources_by_minion_instance": {},
                    "resources_by_pipeline": {},
                    "resource_dependencies_by_dependent_resource": {},
                    "resource_dependents_by_dependency_resource": {},
                    "resource_reference_counts": {},
                }
            else:
                active_receipts = [
                    receipt
                    for receipt in self._result.receipts[: observation.receipt_count]
                    if (
                        receipt.success
                        and receipt.orchestration_id is not None
                        and receipt.instance_id is not None
                        and receipt.directive_index
                        in observation.active_orchestration_start_indexes
                    )
                ]
                orchestrations = frozenset(
                    receipt.orchestration_id
                    for receipt in active_receipts
                    if receipt.orchestration_id is not None
                )
                instance_ids = frozenset(
                    receipt.instance_id
                    for receipt in active_receipts
                    if receipt.instance_id is not None
                )
                pipelines = frozenset(receipt.pipeline_id for receipt in active_receipts)
                directly_owned_resources = {
                    resource_id
                    for receipt in active_receipts
                    for resource_id in (
                        spies.resources_by_minion_id.get(receipt.minion_id, frozenset())
                        | spies.resources_by_pipeline.get(receipt.pipeline_id, frozenset())
                    )
                }
                resources_with_dependencies = set(directly_owned_resources)
                pending_resources = list(directly_owned_resources)
                while pending_resources:
                    resource_id = pending_resources.pop()
                    for dependency_id in spies.resource_dependencies_by_dependent_resource.get(
                        resource_id, frozenset()
                    ):
                        if dependency_id in resources_with_dependencies:
                            continue
                        resources_with_dependencies.add(dependency_id)
                        pending_resources.append(dependency_id)
                resources = frozenset(resources_with_dependencies)
                pipeline_by_minion_instance = {
                    receipt.instance_id: receipt.pipeline_id
                    for receipt in active_receipts
                    if receipt.instance_id is not None
                }
                resources_by_minion_instance = {
                    receipt.instance_id: spies.resources_by_minion_id.get(
                        receipt.minion_id, frozenset()
                    )
                    for receipt in active_receipts
                    if (
                        receipt.instance_id is not None
                        and spies.resources_by_minion_id.get(receipt.minion_id, frozenset())
                    )
                }
                resources_by_pipeline = {
                    pipeline_id: pipeline_resources
                    for pipeline_id in pipelines
                    if (
                        pipeline_resources := spies.resources_by_pipeline.get(
                            pipeline_id, frozenset()
                        )
                    )
                }
                resource_dependencies_by_dependent_resource = {
                    resource_id: dependency_ids
                    for resource_id in resources
                    if (
                        dependency_ids := (
                            spies.resource_dependencies_by_dependent_resource.get(
                                resource_id, frozenset()
                            )
                        )
                    )
                }
                resource_dependents_by_dependency_resource: defaultdict[str, set[str]] = (
                    defaultdict(set)
                )
                for (
                    resource_id,
                    dependency_ids,
                ) in resource_dependencies_by_dependent_resource.items():
                    for dependency_id in dependency_ids:
                        resource_dependents_by_dependency_resource[dependency_id].add(resource_id)

                resource_reference_counts = {resource_id: 0 for resource_id in resources}
                for owner_resources in resources_by_minion_instance.values():
                    for resource_id in owner_resources:
                        resource_reference_counts[resource_id] += 1
                for owner_resources in resources_by_pipeline.values():
                    for resource_id in owner_resources:
                        resource_reference_counts[resource_id] += 1
                for dependency_ids in resource_dependencies_by_dependent_resource.values():
                    for dependency_id in dependency_ids:
                        resource_reference_counts[dependency_id] += 1

                expected_by_state = {
                    "minion_instances": instance_ids,
                    "orchestrations": orchestrations,
                    "minion_tasks": instance_ids,
                    "pipelines": pipelines,
                    "pipeline_tasks": pipelines,
                    "resources": resources,
                    "resource_tasks": resources,
                    "pipeline_by_minion_instance": pipeline_by_minion_instance,
                    "resources_by_minion_instance": (resources_by_minion_instance),
                    "resources_by_pipeline": resources_by_pipeline,
                    "resource_dependencies_by_dependent_resource": (
                        resource_dependencies_by_dependent_resource
                    ),
                    "resource_dependents_by_dependency_resource": {
                        resource_id: frozenset(dependent_ids)
                        for resource_id, dependent_ids in (
                            resource_dependents_by_dependency_resource.items()
                        )
                        if dependent_ids
                    },
                    "resource_reference_counts": resource_reference_counts,
                }

            for state_name, expected in expected_by_state.items():
                actual = getattr(observation.gru_runtime_state, state_name)
                if actual != expected:
                    pytest.fail(
                        "Gru lifecycle tracking mismatch: "
                        f"directive={observation.directive_type.__name__}, "
                        f"observation_index={observation_index}, "
                        f"state={state_name}, "
                        f"expected={expected!r}, "
                        f"actual={actual!r}."
                    )

    def _compute_minion_expectations(
        self,
        spies: SpyRegistry,
    ) -> MinionExpectations:
        return self._compute_minion_expectations_for_receipts(spies, self._result.receipts)

    def _compute_minion_expectations_for_receipts(
        self,
        spies: SpyRegistry,
        receipts: list[OrchestrationStartReceipt],
    ) -> MinionExpectations:
        minion_start_counts: defaultdict[type[SpiedMinion[Any, Any]], int] = defaultdict(int)
        expected_workflows_by_class: defaultdict[type[SpiedMinion[Any, Any]], int] = defaultdict(
            int
        )

        for receipt in receipts:
            directive = self._plan.flat_directives[receipt.directive_index]
            if not isinstance(directive, OrchestrationStart):
                continue

            m_cls = receipt.minion_cls or spies.minions.get(receipt.minion_id)
            if m_cls is None:
                continue

            if receipt.success:
                minion_start_counts[m_cls] += 1
                expected_events = self._plan.pipeline_event_targets.get(receipt.pipeline_id)
                if expected_events is not None:
                    expected_workflows_by_class[m_cls] += expected_events

        return MinionExpectations(
            minion_start_counts=minion_start_counts,
            expected_workflows_by_class=expected_workflows_by_class,
        )

    def _assert_checkpoint_window_workflow_step_progression(self) -> None:
        spies = self._require_spies()
        checkpoints = self._result.checkpoints
        if not checkpoints:
            return

        prev_receipt_count = 0
        prev_counts: dict[str, dict[str, int]] = {}
        prev_counts_by_instance: dict[str, dict[int, dict[str, int]]] = {}
        prev_workflow_step_ids: dict[str, dict[str, tuple[str, ...]]] = {}

        for cp in checkpoints:
            if cp.spy_call_counts is None:
                continue

            if cp.kind != "wait_workflow_completions":
                prev_receipt_count = cp.receipt_count
                prev_counts = cp.spy_call_counts
                prev_counts_by_instance = cp.spy_call_counts_by_instance or {}
                prev_workflow_step_ids = cp.workflow_step_started_ids_by_minion_id or {}
                continue

            if cp.orchestration_directive_indexes == ():
                continue
            mode = cp.workflow_steps_mode or "at_least"
            if mode not in ("at_least", "exact"):
                pytest.fail(
                    "WaitWorkflowCompletions.workflow_steps_mode="
                    f"{mode!r} is unsupported. Use 'at_least' or 'exact'."
                )

            window_receipts = self._result.receipts[prev_receipt_count:cp.receipt_count]
            if cp.orchestration_directive_indexes is not None:
                allowed_indexes = set(cp.orchestration_directive_indexes)
                window_receipts = [
                    r for r in window_receipts
                    if r.directive_index in allowed_indexes
                ]
            window_expectations = self._compute_minion_expectations_for_receipts(
                spies, window_receipts
            )

            for (
                m_cls,
                expected_workflows,
            ) in window_expectations.expected_workflows_by_class.items():
                if expected_workflows < 0:
                    pytest.fail(
                        f"Invalid expected workflow count for {m_cls.__name__} in "
                        f"checkpoint {cp.order}: {expected_workflows}"
                    )

                key = f"{m_cls.__module__}.{m_cls.__name__}"
                minion_ids = {
                    r.minion_id
                    for r in window_receipts
                    if r.success
                    and (
                        r.minion_cls is m_cls
                        or (r.minion_cls is None and spies.minions.get(r.minion_id) is m_cls)
                    )
                }
                curr = cp.spy_call_counts.get(key, {})
                prev = prev_counts.get(key, {})
                curr_by_instance = (cp.spy_call_counts_by_instance or {}).get(key, {})
                prev_by_instance = prev_counts_by_instance.get(key, {})
                curr_workflow_ids_by_step = self._merge_workflow_step_ids_by_step(
                    cp.workflow_step_started_ids_by_minion_id,
                    minion_ids,
                )
                prev_workflow_ids_by_step = self._merge_workflow_step_ids_by_step(
                    prev_workflow_step_ids,
                    minion_ids,
                )

                workflow = self._require_workflow_spec(m_cls)
                for step_name in workflow:
                    expected_delta = expected_workflows
                    start_count_in_window = window_expectations.minion_start_counts.get(m_cls, 0)
                    max_tolerated_delta = expected_delta + start_count_in_window
                    actual_delta = curr.get(step_name, 0) - prev.get(step_name, 0)
                    workflow_id_delta = self._workflow_id_delta_count(
                        curr=curr_workflow_ids_by_step.get(step_name, ()),
                        prev=prev_workflow_ids_by_step.get(step_name, ()),
                    )
                    has_workflow_id_evidence = cp.workflow_step_started_ids_by_minion_id is not None
                    check = self._evaluate_window_step_progression(
                        mode=mode,
                        expected_delta=expected_delta,
                        max_tolerated_delta=max_tolerated_delta,
                        actual_delta=actual_delta,
                        workflow_id_delta=workflow_id_delta,
                        has_workflow_id_evidence=has_workflow_id_evidence,
                    )
                    if check.has_workflow_id_evidence and check.workflow_id_mismatch:
                        instance_deltas = self._instance_step_deltas(
                            step_name=step_name,
                            curr_by_instance=curr_by_instance,
                            prev_by_instance=prev_by_instance,
                        )
                        workflow_id_expected = self._format_expected_range(
                            check.workflow_id_expected_min,
                            check.workflow_id_expected_max,
                        )
                        call_expected = self._format_expected_range(
                            check.call_expected_min,
                            check.call_expected_max,
                        )
                        pytest.fail(
                            "Checkpoint workflow-id progression mismatch for "
                            f"{m_cls.__name__}.{step_name} at checkpoint {cp.order}: "
                            "expected workflow-id delta "
                            f"{workflow_id_expected}, got {check.workflow_id_actual}. "
                            f"Call-count delta: {check.call_actual} (expected "
                            f"{call_expected}). "
                            f"Per-instance deltas: {instance_deltas}"
                        )
                    if check.call_mismatch:
                        instance_deltas = self._instance_step_deltas(
                            step_name=step_name,
                            curr_by_instance=curr_by_instance,
                            prev_by_instance=prev_by_instance,
                        )
                        call_expected = self._format_expected_range(
                            check.call_expected_min,
                            check.call_expected_max,
                        )
                        workflow_id_expected = self._format_expected_range(
                            check.workflow_id_expected_min,
                            check.workflow_id_expected_max,
                        )
                        pytest.fail(
                            "Checkpoint workflow-step progression mismatch for "
                            f"{m_cls.__name__}.{step_name} at checkpoint {cp.order}: "
                            "expected call-count delta "
                            f"{call_expected}, got {check.call_actual}. "
                            f"Workflow-id delta: {check.workflow_id_actual} (expected "
                            f"{workflow_id_expected}). "
                            f"Per-instance deltas: {instance_deltas}."
                        )

            prev_receipt_count = cp.receipt_count
            prev_counts = cp.spy_call_counts
            prev_counts_by_instance = cp.spy_call_counts_by_instance or {}
            prev_workflow_step_ids = cp.workflow_step_started_ids_by_minion_id or {}

    def _assert_workflow_step_start_events_are_monotonic(self) -> None:
        spies = self._require_spies()
        latest_checkpoint = next(
            (
                cp
                for cp in reversed(self._result.checkpoints)
                if cp.workflow_step_start_events_by_minion_id is not None
            ),
            None,
        )
        if (
            latest_checkpoint is None
            or latest_checkpoint.workflow_step_start_events_by_minion_id is None
        ):
            return

        workflow_by_minion_id = latest_checkpoint.workflow_step_start_events_by_minion_id
        for m_cls in spies.minions.values():
            workflow = self._require_workflow_spec(m_cls)

            minion_ids = {
                r.minion_id
                for r in self._result.receipts[:latest_checkpoint.receipt_count]
                if r.success
                and (
                    r.minion_cls is m_cls
                    or (r.minion_cls is None and spies.minions.get(r.minion_id) is m_cls)
                )
            }
            for workflow_id, events in self._merge_workflow_step_events_by_workflow_id(
                workflow_by_minion_id,
                minion_ids,
            ).items():
                previous_index = -1
                previous_step_name: str | None = None
                for step_index, step_name in events:
                    if step_index < 0 or step_index >= len(workflow):
                        pytest.fail(
                            "Workflow step start event has out-of-bounds step_index for "
                            f"{m_cls.__name__}: workflow_id={workflow_id!r}, "
                            f"step_index={step_index}, workflow_len={len(workflow)}."
                        )

                    expected_step_name = workflow[step_index]
                    if step_name != expected_step_name:
                        pytest.fail(
                            "Workflow step start event step_name/index mismatch for "
                            f"{m_cls.__name__}: workflow_id={workflow_id!r}, "
                            f"step_index={step_index}, expected {expected_step_name!r}, "
                            f"got {step_name!r}."
                        )

                    if step_index < previous_index:
                        pytest.fail(
                            "Workflow step start events are not monotonic for "
                            f"{m_cls.__name__}: workflow_id={workflow_id!r}, "
                            f"previous={previous_index}:{previous_step_name}, "
                            f"current={step_index}:{step_name}."
                        )

                    if step_index == previous_index and step_name != previous_step_name:
                        pytest.fail(
                            "Workflow step start events repeat a step_index with "
                            "different names for "
                            f"{m_cls.__name__}: workflow_id={workflow_id!r}, "
                            f"previous={previous_index}:{previous_step_name}, "
                            f"current={step_index}:{step_name}."
                        )

                    previous_index = step_index
                    previous_step_name = step_name

    def _evaluate_window_step_progression(
        self,
        *,
        mode: str,
        expected_delta: int,
        max_tolerated_delta: int,
        actual_delta: int,
        workflow_id_delta: int,
        has_workflow_id_evidence: bool,
    ) -> WindowStepProgressionCheck:
        if mode == "at_least":
            call_expected_min = expected_delta
            call_expected_max = -1
            workflow_id_expected_min = expected_delta
            workflow_id_expected_max = -1
            call_mismatch = actual_delta < call_expected_min
            workflow_id_mismatch = workflow_id_delta < workflow_id_expected_min
            return WindowStepProgressionCheck(
                call_expected_min=call_expected_min,
                call_expected_max=call_expected_max,
                call_actual=actual_delta,
                workflow_id_expected_min=workflow_id_expected_min,
                workflow_id_expected_max=workflow_id_expected_max,
                workflow_id_actual=workflow_id_delta,
                has_workflow_id_evidence=has_workflow_id_evidence,
                workflow_id_mismatch=workflow_id_mismatch,
                call_mismatch=call_mismatch,
            )

        # exact mode
        if has_workflow_id_evidence:
            call_expected_min = expected_delta
            call_expected_max = max_tolerated_delta
        else:
            call_expected_min = expected_delta
            call_expected_max = expected_delta
        workflow_id_expected_min = expected_delta
        workflow_id_expected_max = expected_delta

        call_mismatch = (
            actual_delta < call_expected_min
            or actual_delta > call_expected_max
        )
        workflow_id_mismatch = workflow_id_delta != workflow_id_expected_min
        return WindowStepProgressionCheck(
            call_expected_min=call_expected_min,
            call_expected_max=call_expected_max,
            call_actual=actual_delta,
            workflow_id_expected_min=workflow_id_expected_min,
            workflow_id_expected_max=workflow_id_expected_max,
            workflow_id_actual=workflow_id_delta,
            has_workflow_id_evidence=has_workflow_id_evidence,
            workflow_id_mismatch=workflow_id_mismatch,
            call_mismatch=call_mismatch,
        )

    def _format_expected_range(self, expected_min: int, expected_max: int) -> str:
        if expected_max < 0:
            return f">= {expected_min}"
        if expected_min == expected_max:
            return str(expected_min)
        return f"{expected_min}..{expected_max}"

    def _instance_step_deltas(
        self,
        *,
        step_name: str,
        curr_by_instance: dict[int, dict[str, int]],
        prev_by_instance: dict[int, dict[str, int]],
    ) -> dict[int, int]:
        deltas: dict[int, int] = {}
        tags = set(curr_by_instance) | set(prev_by_instance)
        for tag in tags:
            curr_val = curr_by_instance.get(tag, {}).get(step_name, 0)
            prev_val = prev_by_instance.get(tag, {}).get(step_name, 0)
            delta = curr_val - prev_val
            if delta != 0:
                deltas[tag] = delta
        return dict(sorted(deltas.items()))

    def _workflow_id_delta_count(
        self,
        *,
        curr: tuple[str, ...],
        prev: tuple[str, ...],
    ) -> int:
        curr_ids = set(curr)
        prev_ids = set(prev)
        return len(curr_ids - prev_ids)

    def _merge_workflow_step_ids_by_step(
        self,
        workflow_step_ids_by_minion_id: dict[str, dict[str, tuple[str, ...]]] | None,
        minion_ids: set[str],
    ) -> dict[str, tuple[str, ...]]:
        if workflow_step_ids_by_minion_id is None:
            return {}
        merged: defaultdict[str, set[str]] = defaultdict(set)
        for minion_id in minion_ids:
            for step_name, workflow_ids in workflow_step_ids_by_minion_id.get(
                minion_id, {}
            ).items():
                merged[step_name].update(workflow_ids)
        return {
            step_name: tuple(sorted(workflow_ids)) for step_name, workflow_ids in merged.items()
        }

    def _merge_workflow_step_events_by_workflow_id(
        self,
        workflow_step_events_by_minion_id: dict[str, dict[str, tuple[tuple[int, str], ...]]] | None,
        minion_ids: set[str],
    ) -> dict[str, tuple[tuple[int, str], ...]]:
        if workflow_step_events_by_minion_id is None:
            return {}
        merged: defaultdict[str, list[tuple[int, str]]] = defaultdict(list)
        for minion_id in minion_ids:
            for workflow_id, events in workflow_step_events_by_minion_id.get(minion_id, {}).items():
                merged[workflow_id].extend(events)
        return {
            workflow_id: tuple(events)
            for workflow_id, events in merged.items()
        }

    def _counter_sample_value(self, sample: dict[str, object]) -> int:
        value = sample.get("value", 0)
        if isinstance(value, int | float | str):
            return int(value)
        return 0

    def _counter_sample_label(self, sample: dict[str, object], label_name: str) -> object:
        labels = sample.get("labels", {})

        if not isinstance(labels, dict):
            return None

        for k, v in cast(dict[Any, Any], labels).items():
            if str(k) == label_name:
                return v

        return None

    def _assert_runtime_expectations(self) -> None:
        expect_directives = [
            d for d in self._plan.flat_directives
            if isinstance(d, ExpectRuntime)
        ]
        expect_checkpoints = [
            cp for cp in self._result.checkpoints
            if cp.kind == "expect_runtime"
        ]

        if len(expect_directives) != len(expect_checkpoints):
            pytest.fail(
                "ExpectRuntime directive/checkpoint mismatch: "
                f"{len(expect_directives)} directive(s), {len(expect_checkpoints)} checkpoint(s)."
            )

        for directive, checkpoint in zip(expect_directives, expect_checkpoints):
            target_checkpoint = self._resolve_expect_runtime_checkpoint_target(
                directive=directive,
                own_checkpoint=checkpoint,
            )
            persistence = directive.expect.persistence
            resolutions = directive.expect.resolutions
            workflow_steps = directive.expect.workflow_steps
            workflow_steps_mode = directive.expect.workflow_steps_mode

            receipts = self._result.receipts[:target_checkpoint.receipt_count]
            receipts_by_directive_index = {
                receipt.directive_index: receipt
                for receipt in receipts
                if receipt.success
            }

            def receipt_for(start: OrchestrationStart, section: str) -> OrchestrationStartReceipt:
                directive_index = self._plan.directive_index(start)
                receipt = receipts_by_directive_index.get(directive_index)
                if receipt is None:
                    pytest.fail(
                        f"ExpectRuntime.{section} references a start with no successful "
                        f"receipt at checkpoint {target_checkpoint.order}: {directive_index}."
                    )
                return receipt

            if persistence:
                persisted = target_checkpoint.persisted_contexts_by_orchestration_id
                if persisted is None:
                    pytest.fail(
                        "ExpectRuntime.persistence is unsupported with this "
                        "StateStore snapshot strategy."
                    )

                for start, expected_count in persistence.items():
                    receipt = receipt_for(start, "persistence")
                    if expected_count < 0:
                        pytest.fail(
                            "ExpectRuntime.persistence counts must be ints >= 0; "
                            f"got {expected_count!r}."
                        )

                    orchestration_id = receipt.orchestration_id
                    actual_count = (
                        0 if orchestration_id is None else persisted.get(orchestration_id, 0)
                    )
                    if actual_count != expected_count:
                        pytest.fail(
                            f"ExpectRuntime.persistence mismatch for start "
                            f"{receipt.directive_index}: expected {expected_count}, "
                            f"got {actual_count}."
                        )

            if resolutions:
                counters = target_checkpoint.metrics_counters
                if counters is None:
                    pytest.fail(
                        "ExpectRuntime.resolutions is unsupported with this "
                        "Metrics snapshot strategy."
                    )

                for start, expected_status_counts in resolutions.items():
                    receipt = receipt_for(start, "resolutions")
                    orchestration_id = receipt.orchestration_id
                    counts = {
                        "succeeded": sum(
                            self._counter_sample_value(s)
                            for s in counters.get(MINION_WORKFLOW_SUCCEEDED_TOTAL, [])
                            if self._counter_sample_label(s, LABEL_ORCHESTRATION_ID)
                            == orchestration_id
                        ),
                        "failed": sum(
                            self._counter_sample_value(s)
                            for s in counters.get(MINION_WORKFLOW_FAILED_TOTAL, [])
                            if self._counter_sample_label(s, LABEL_ORCHESTRATION_ID)
                            == orchestration_id
                        ),
                        "aborted": sum(
                            self._counter_sample_value(s)
                            for s in counters.get(MINION_WORKFLOW_ABORTED_TOTAL, [])
                            if self._counter_sample_label(s, LABEL_ORCHESTRATION_ID)
                            == orchestration_id
                        ),
                    }

                    for status, expected_count in expected_status_counts.items():
                        if status not in counts:
                            pytest.fail(
                                f"ExpectRuntime.resolutions has unknown status {status!r} "
                                f"for start {receipt.directive_index}."
                            )
                        if expected_count < 0:
                            pytest.fail(
                                "ExpectRuntime.resolutions counts must be ints >= 0; "
                                f"got start {receipt.directive_index}.{status}={expected_count!r}."
                            )
                        actual = counts[status]
                        if actual != expected_count:
                            pytest.fail(
                                "ExpectRuntime.resolutions mismatch for "
                                f"start {receipt.directive_index}.{status}: "
                                f"expected {expected_count}, got {actual}."
                            )

            if workflow_steps:
                if workflow_steps_mode not in ("at_least", "exact"):
                    pytest.fail(
                        "ExpectRuntime.workflow_steps_mode="
                        f"{workflow_steps_mode!r} is unsupported. Use 'at_least' or 'exact'."
                    )
                workflow_step_ids = (
                    target_checkpoint.workflow_step_started_ids_by_orchestration_id
                )
                if workflow_step_ids is None:
                    pytest.fail(
                        "ExpectRuntime.workflow_steps is unsupported without workflow step-id "
                        "checkpoint snapshots."
                    )

                for start, expected_steps in workflow_steps.items():
                    receipt = receipt_for(start, "workflow_steps")
                    orchestration_id = receipt.orchestration_id
                    for step_name, expected_count in expected_steps.items():
                        if expected_count < 0:
                            pytest.fail(
                                "ExpectRuntime.workflow_steps counts must be ints >= 0; "
                                f"got start {receipt.directive_index}.{step_name}="
                                f"{expected_count!r}."
                            )
                        actual_count = len(
                            workflow_step_ids.get(orchestration_id or "", {}).get(
                                step_name,
                                (),
                            )
                        )
                        if workflow_steps_mode == "at_least":
                            if actual_count < expected_count:
                                pytest.fail(
                                    "ExpectRuntime.workflow_steps mismatch for "
                                    f"start {receipt.directive_index}.{step_name}: "
                                    f"expected >= {expected_count}, got {actual_count}."
                                )
                        else:
                            if actual_count != expected_count:
                                pytest.fail(
                                    "ExpectRuntime.workflow_steps mismatch for "
                                    f"start {receipt.directive_index}.{step_name}: "
                                    f"expected {expected_count}, got {actual_count}."
                                )

    def _resolve_expect_runtime_checkpoint_target(
        self,
        *,
        directive: ExpectRuntime,
        own_checkpoint: ScenarioCheckpoint,
    ) -> ScenarioCheckpoint:
        if directive.at == "latest":
            return own_checkpoint

        at = directive.at
        if isinstance(at, int) and not isinstance(at, bool):
            checkpoints = self._result.checkpoints
            if at < 0 or at >= len(checkpoints):
                pytest.fail(
                    f"ExpectRuntime.at index {at} is out of range for "
                    f"{len(checkpoints)} checkpoint(s)."
                )
            return checkpoints[at]

        pytest.fail(
            f"ExpectRuntime.at={directive.at!r} is unsupported. Use 'latest' or a checkpoint index."
        )

    def _assert_minion_fanout_delivery(self) -> None:
        """Assert explicit per-minion fanout delivery from pipeline event targets.

        For each minion class with successful starts, each workflow step is expected
        to execute within explicit runtime-tolerated bounds:
        expected workflows per successful starts, allowing +1 per start.
        """
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)

        for m_cls, expected_workflows in expectations.expected_workflows_by_class.items():
            if expected_workflows < 0:
                pytest.fail(
                    f"Invalid expected workflow count for {m_cls.__name__}: {expected_workflows}"
                )
            start_count = expectations.minion_start_counts.get(m_cls, 0)
            max_expected_workflows = expected_workflows + start_count

            actual_counts = m_cls.get_call_counts()

            workflow = self._require_workflow_spec(m_cls)
            for step_name in workflow:
                actual = actual_counts.get(step_name, 0)
                if actual < expected_workflows or actual > max_expected_workflows:
                    pytest.fail(
                        f"Fanout mismatch for {m_cls.__name__}.{step_name}: "
                        "expected "
                        f"{expected_workflows}..{max_expected_workflows} workflow calls "
                        f"from pipeline events, got {actual}."
                    )

    def _assert_pipeline_events(self) -> None:
        spies = self._require_spies()
        outcomes = self._compute_pipeline_attempt_outcomes()
        for pipeline_id, p_cls in spies.pipelines.items():
            expected_events = self._plan.pipeline_event_targets.get(pipeline_id)
            counts = p_cls.get_call_counts()

            attempts = max(
                outcomes.attempts_by_pipeline_id.get(pipeline_id, 0),
                spies.pipeline_start_attempt_counts.get(pipeline_id, 0),
            )
            successes = outcomes.successes_by_pipeline_id.get(pipeline_id, 0)

            min_started = 1 if successes > 0 else 0
            max_started = attempts

            actual_inits = counts.get("__init__", 0)
            min_inits = 1 if successes > 0 else 0
            max_inits = attempts
            if actual_inits < min_inits or actual_inits > max_inits:
                pytest.fail(
                    f"{p_cls.__name__} __init__ mismatch: expected "
                    f"{min_inits}..{max_inits}, got {actual_inits}"
                )

            actual_startup = counts.get("startup", 0)
            if actual_startup < min_started or actual_startup > max_started:
                pytest.fail(
                    f"{p_cls.__name__} startup mismatch: expected "
                    f"{min_started}..{max_started}, got {actual_startup}"
                )

            actual_run = counts.get("run", 0)
            if actual_run < min_started or actual_run > actual_startup:
                pytest.fail(
                    f"{p_cls.__name__} run mismatch: expected "
                    f"{min_started}..{actual_startup}, got {actual_run}"
                )

            if self._result.seen_shutdown:
                actual_shutdown = counts.get("shutdown", 0)
                if actual_shutdown < 0 or actual_shutdown > actual_startup:
                    pytest.fail(
                        f"{p_cls.__name__} shutdown mismatch: expected 0.."
                        f"{actual_startup}, got {actual_shutdown}"
                    )

            if expected_events is not None:
                actual = counts.get("produce_event", 0)
                if actual_startup > 0:
                    min_events = expected_events * actual_startup
                    max_events = (expected_events + 1) * actual_startup
                    if actual < min_events or actual > max_events:
                        pytest.fail(
                            f"{p_cls.__name__} produce_event mismatch: "
                            f"expected {min_events}..{max_events}, got {actual}"
                        )
                elif actual != 0:
                    pytest.fail(
                        f"{p_cls.__name__} produce_event mismatch: expected 0 when "
                        f"no starts succeeded, got {actual}"
                    )

    def _compute_pipeline_attempt_outcomes(self) -> PipelineAttemptOutcomes:
        attempts_by_pipeline_id: defaultdict[str, int] = defaultdict(int)
        successes_by_pipeline_id: defaultdict[str, int] = defaultdict(int)
        for receipt in self._result.receipts:
            attempts_by_pipeline_id[receipt.pipeline_id] += 1
            if receipt.success:
                successes_by_pipeline_id[receipt.pipeline_id] += 1
        return PipelineAttemptOutcomes(
            attempts_by_pipeline_id=attempts_by_pipeline_id,
            successes_by_pipeline_id=successes_by_pipeline_id,
        )

    async def _pin_and_assert_calls(
        self,
        call_counts: dict[
            type[SpiedMinion[Any, Any]] | type[SpiedResource] | type[SpiedStateStore],
            dict[str, int],
        ],
        allow_unlisted: set[type[SpiedPipeline[Any]] | type[SpiedResource]],
    ) -> list[Callable[[], None]]:
        try:
            return await asyncio.gather(
                *[
                    cls.await_and_pin_call_counts(
                        expected=counts,
                        on_extra=_ExtraCallRecorder(self._result, cls),
                        allow_unlisted=(cls in allow_unlisted),
                        timeout=self._timeout,
                    )
                    for cls, counts in call_counts.items()
                ]
            )
        except Exception:
            mismatches: list[str] = []
            for cls, expected in call_counts.items():
                actual = cls.get_call_counts()
                diff = {
                    name: (actual.get(name, 0), count)
                    for name, count in expected.items()
                    if actual.get(name, 0) != count
                }
                if diff:
                    mismatches.append(f"{cls.__name__}: {diff}")
            pytest.fail("Call counts did not reach expected values. " + "; ".join(mismatches))

    def _assert_call_order(
        self,
        call_counts: dict[
            type[SpiedMinion[Any, Any]] | type[SpiedResource] | type[SpiedStateStore],
            dict[str, int],
        ],
    ) -> None:
        spies = self._require_spies()
        minion_start_counts = self._compute_minion_expectations(spies).minion_start_counts
        ss_tag = getattr(self._state_store, "_mspy_instance_tag", None)
        if ss_tag is not None:
            self._result.instance_tags[type(self._state_store)].add(ss_tag)

        for m_cls in spies.minions.values():
            if minion_start_counts.get(m_cls, 0) <= 0:
                continue
            expected_calls = call_counts.get(m_cls, {})
            workflow = self._require_workflow_spec(m_cls)
            workflow_steps = [
                name
                for name in workflow
                if expected_calls.get(name, 0) > 0
            ]
            for tag in self._result.instance_tags.get(m_cls, set()):
                names = _names_for_tag(m_cls, tag)
                order = ["__init__", "startup", "run", *workflow_steps]
                if "shutdown" in names:
                    order.append("shutdown")
                m_cls.assert_call_order_for_instance(tag, order)

        for pipeline_id, p_cls in spies.pipelines.items():
            expected_events = self._plan.pipeline_event_targets.get(pipeline_id)
            for tag in self._result.instance_tags.get(p_cls, set()):
                names = _names_for_tag(p_cls, tag)
                order = ["__init__", "startup", "run"]
                if "produce_event" in names or expected_events:
                    order.append("produce_event")
                if "shutdown" in names:
                    order.append("shutdown")
                p_cls.assert_call_order_for_instance(tag, order)

        for r_cls in spies.resources:
            for tag in self._result.instance_tags.get(r_cls, set()):
                names = _names_for_tag(r_cls, tag)
                order = ["__init__", "startup", "run"]
                if "shutdown" in names:
                    order.append("shutdown")
                r_cls.assert_call_order_for_instance(tag, order)

        if ss_tag is not None:
            names = _names_for_tag(type(self._state_store), ss_tag)
            order = ["__init__", "startup"]
            if "shutdown" in names:
                order.append("shutdown")
            type(self._state_store).assert_call_order_for_instance(ss_tag, order)

        if self._result.extra_calls:
            messages = [
                f"{cls.__name__}: args={args} kwargs={kwargs}"
                for cls, args, kwargs in self._result.extra_calls
            ]
            pytest.fail("Unexpected extra calls detected: " + "; ".join(messages))

    def _require_spies(self) -> SpyRegistry:
        if self._result.spies is None:
            raise AssertionError(
                "ScenarioVerifier internal invariant violated: result.spies is None. "
                "Ensure verification runs on a ScenarioRunResult produced by ScenarioRunner.run()."
            )
        return self._result.spies
