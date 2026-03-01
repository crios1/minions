import asyncio
from collections import defaultdict
from dataclasses import dataclass
from typing import Callable

import pytest

from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_ABORTED_TOTAL,
    MINION_WORKFLOW_FAILED_TOTAL,
    MINION_WORKFLOW_SUCCEEDED_TOTAL,
    LABEL_MINION_INSTANCE_ID,
)

from tests.assets.support.mixin_spy import SpyMixin
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore

from .directives import ExpectRuntime, MinionStart
from .plan import ScenarioPlan
from .runner import ScenarioCheckpoint, ScenarioRunResult, SpyRegistry, StartReceipt


class _ExtraCallRecorder:
    def __init__(self, result: ScenarioRunResult, cls: type[SpyMixin]):
        self._result = result
        self._cls = cls

    def __call__(self, *a, **kw) -> None:
        self._result.extra_calls.append((self._cls, a, kw))


def _names_for_tag(cls: type[SpyMixin], tag: int) -> list[str]:
    return [n for n, _, t in cls.get_call_history() if t == tag]


@dataclass(frozen=True)
class MinionExpectations:
    """Computed expectations for minion starts and workflow calls."""

    minion_start_counts: defaultdict[type[SpyMixin], int]
    expected_workflows_by_class: defaultdict[type[SpyMixin], int]


@dataclass(frozen=True)
class ExpectedCallCounts:
    """Expected call counts plus spy classes allowed to have extra calls."""

    call_counts: dict[type[SpyMixin], dict[str, int]]
    allow_unlisted: set[type[SpyMixin]]


@dataclass(frozen=True)
class PipelineAttemptOutcomes:
    attempts_by_modpath: defaultdict[str, int]
    successes_by_modpath: defaultdict[str, int]


class ScenarioVerifier:
    def __init__(
        self,
        plan: ScenarioPlan,
        result: ScenarioRunResult,
        *,
        logger: InMemoryLogger,
        metrics: InMemoryMetrics,
        state_store: InMemoryStateStore,
        per_verification_timeout: float,
    ):
        self._plan = plan
        self._result = result
        self._logger = logger
        self._metrics = metrics
        self._state_store = state_store
        self._timeout = per_verification_timeout

    async def verify(self) -> None:
        expected = self._build_expected_call_counts()
        self._assert_runtime_expectations()
        self._assert_pipeline_events()
        self._assert_checkpoint_window_workflow_step_progression()

        unpin_fns: list[Callable[[], None]] = []
        try:
            unpin_fns = await self._pin_and_assert_calls(expected.call_counts, expected.allow_unlisted)
            self._assert_state_store_read_call_bounds()
            self._assert_minion_fanout_delivery()
            self._assert_call_order(expected.call_counts)
        finally:
            for unpin in unpin_fns:
                unpin()

    def _build_expected_call_counts(self) -> ExpectedCallCounts:
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)
        call_counts: dict[type[SpyMixin], dict[str, int]] = {}
        allow_unlisted: set[type[SpyMixin]] = set()

        for r_cls in spies.resources:
            call_counts[r_cls] = {"__init__": 1, "startup": 1, "run": 1}
            allow_unlisted.add(r_cls)

        for p_cls in spies.pipelines.values():
            allow_unlisted.add(p_cls)

        for m_cls in spies.minions.values():
            starts = expectations.minion_start_counts.get(m_cls, 0)
            base = {
                "__init__": starts,
                "startup": starts,
                "run": starts,
                **{
                    name: expectations.expected_workflows_by_class.get(m_cls, 0)
                    for name in m_cls._mn_workflow_spec  # type: ignore
                },
            }
            call_counts[m_cls] = base

        minion_starts = sum(expectations.minion_start_counts.values())
        workflows_started = sum(expectations.expected_workflows_by_class.values())
        unresolved_workflows = self._count_unresolved_persisted_workflows(spies)
        resolved_workflows = max(workflows_started - unresolved_workflows, 0)
        workflow_steps = sum(
            len(m._mn_workflow_spec) * expectations.expected_workflows_by_class.get(m, 0)  # type: ignore
            for m in spies.minions.values()
        )

        ss = {
            "__init__": 1,
            "startup": 1,
            "get_contexts_for_minion": minion_starts,
            "_get_contexts_for_minion": minion_starts,
            "save_context": workflows_started + workflow_steps,
            "_save_context": workflow_steps,
            "delete_context": resolved_workflows,
            "_delete_context": resolved_workflows,
        }
        if self._result.seen_shutdown:
            ss["shutdown"] = 1
        call_counts[type(self._state_store)] = ss

        return ExpectedCallCounts(call_counts=call_counts, allow_unlisted=allow_unlisted)

    def _count_unresolved_persisted_workflows(self, spies: SpyRegistry) -> int:
        latest_with_persistence = next(
            (cp for cp in reversed(self._result.checkpoints) if cp.persisted_contexts_by_modpath is not None),
            None,
        )
        if latest_with_persistence is None or latest_with_persistence.persisted_contexts_by_modpath is None:
            return 0
        persisted = latest_with_persistence.persisted_contexts_by_modpath
        tracked_modpaths = set(spies.minions.keys())
        return sum(count for modpath, count in persisted.items() if modpath in tracked_modpaths)

    def _assert_state_store_read_call_bounds(self) -> None:
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)
        minion_starts = sum(expectations.minion_start_counts.values())
        state_store_counts = type(self._state_store).get_call_counts()
        get_all_calls = state_store_counts.get("get_all_contexts", 0)
        if get_all_calls > minion_starts:
            pytest.fail(
                "StateStore.get_all_contexts called more times than minion starts: "
                f"{get_all_calls} > {minion_starts}"
            )

    def _compute_minion_expectations(
        self,
        spies: SpyRegistry,
    ) -> MinionExpectations:
        return self._compute_minion_expectations_for_receipts(spies, self._result.receipts)

    def _compute_minion_expectations_for_receipts(
        self,
        spies: SpyRegistry,
        receipts: list[StartReceipt],
    ) -> MinionExpectations:
        minion_start_counts: defaultdict[type[SpyMixin], int] = defaultdict(int)
        expected_workflows_by_class: defaultdict[type[SpyMixin], int] = defaultdict(int)

        for receipt in receipts:
            directive = self._plan.flat_directives[receipt.directive_index]
            if not isinstance(directive, MinionStart):
                continue

            m_cls = receipt.minion_cls or spies.minions.get(receipt.minion_modpath)
            if m_cls is None:
                continue

            if receipt.success:
                minion_start_counts[m_cls] += 1
                expected_events = self._plan.pipeline_event_targets.get(receipt.pipeline_modpath)
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
                prev_workflow_step_ids = cp.workflow_step_started_ids_by_class or {}
                continue

            if cp.minion_names == ():
                continue
            mode = cp.workflow_steps_mode or "at_least"
            if mode not in ("at_least", "exact"):
                pytest.fail(
                    "WaitWorkflowCompletions.workflow_steps_mode="
                    f"{mode!r} is unsupported. Use 'at_least' or 'exact'."
                )

            window_receipts = self._result.receipts[prev_receipt_count:cp.receipt_count]
            if cp.minion_names is not None:
                allowed_names = set(cp.minion_names)
                window_receipts = [
                    r for r in window_receipts
                    if r.resolved_name in allowed_names
                ]
            window_expectations = self._compute_minion_expectations_for_receipts(spies, window_receipts)

            for m_cls, expected_workflows in window_expectations.expected_workflows_by_class.items():
                if expected_workflows < 0:
                    pytest.fail(
                        f"Invalid expected workflow count for {m_cls.__name__} in checkpoint {cp.order}: "
                        f"{expected_workflows}"
                    )

                key = f"{m_cls.__module__}.{m_cls.__name__}"
                curr = cp.spy_call_counts.get(key, {})
                prev = prev_counts.get(key, {})
                curr_by_instance = (cp.spy_call_counts_by_instance or {}).get(key, {})
                prev_by_instance = prev_counts_by_instance.get(key, {})
                curr_workflow_ids_by_step = (cp.workflow_step_started_ids_by_class or {}).get(key, {})
                prev_workflow_ids_by_step = prev_workflow_step_ids.get(key, {})

                for step_name in m_cls._mn_workflow_spec:  # type: ignore
                    expected_delta = expected_workflows
                    actual_delta = curr.get(step_name, 0) - prev.get(step_name, 0)
                    workflow_id_delta = self._workflow_id_delta_count(
                        curr=curr_workflow_ids_by_step.get(step_name, ()),
                        prev=prev_workflow_ids_by_step.get(step_name, ()),
                    )
                    has_workflow_id_evidence = cp.workflow_step_started_ids_by_class is not None
                    call_count_mismatch = (
                        actual_delta < expected_delta
                        if mode == "at_least" or has_workflow_id_evidence
                        else actual_delta != expected_delta
                    )
                    if call_count_mismatch:
                        instance_deltas = self._instance_step_deltas(
                            step_name=step_name,
                            curr_by_instance=curr_by_instance,
                            prev_by_instance=prev_by_instance,
                        )
                        expected_phrase = (
                            f">= {expected_delta}" if mode == "at_least" else str(expected_delta)
                        )
                        pytest.fail(
                            "Checkpoint workflow-step progression mismatch for "
                            f"{m_cls.__name__}.{step_name} at checkpoint {cp.order}: "
                            f"expected delta {expected_phrase}, got {actual_delta}. "
                            f"Per-instance deltas: {instance_deltas}. "
                            f"Workflow-id delta: {workflow_id_delta}"
                        )
                    workflow_id_mismatch = (
                        workflow_id_delta < expected_delta
                        if mode == "at_least"
                        else workflow_id_delta != expected_delta
                    )
                    if has_workflow_id_evidence and workflow_id_mismatch:
                        instance_deltas = self._instance_step_deltas(
                            step_name=step_name,
                            curr_by_instance=curr_by_instance,
                            prev_by_instance=prev_by_instance,
                        )
                        expected_workflow_id_phrase = (
                            f">= {expected_delta}" if mode == "at_least" else str(expected_delta)
                        )
                        pytest.fail(
                            "Checkpoint workflow-id progression mismatch for "
                            f"{m_cls.__name__}.{step_name} at checkpoint {cp.order}: "
                            "expected workflow-id delta "
                            f"{expected_workflow_id_phrase}, got {workflow_id_delta}. "
                            f"Call-count delta: {actual_delta}. "
                            f"Per-instance deltas: {instance_deltas}"
                        )

            prev_receipt_count = cp.receipt_count
            prev_counts = cp.spy_call_counts
            prev_counts_by_instance = cp.spy_call_counts_by_instance or {}
            prev_workflow_step_ids = cp.workflow_step_started_ids_by_class or {}

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
            modpaths_by_name: defaultdict[str, set[str]] = defaultdict(set)
            instance_ids_by_name: defaultdict[str, set[str]] = defaultdict(set)
            for r in receipts:
                if not r.success or not r.resolved_name:
                    continue
                modpaths_by_name[r.resolved_name].add(r.minion_modpath)
                if r.instance_id:
                    instance_ids_by_name[r.resolved_name].add(r.instance_id)

            if persistence:
                persisted = target_checkpoint.persisted_contexts_by_modpath
                if persisted is None:
                    pytest.fail(
                        "ExpectRuntime.persistence is unsupported with this StateStore snapshot strategy."
                    )

                for minion_name, expected_count in persistence.items():
                    if minion_name not in modpaths_by_name:
                        pytest.fail(
                            f"ExpectRuntime.persistence references unknown minion name: {minion_name!r}."
                        )
                    if not isinstance(expected_count, int) or expected_count < 0:
                        pytest.fail(
                            "ExpectRuntime.persistence counts must be ints >= 0; "
                            f"got {minion_name!r}={expected_count!r}."
                        )

                    modpaths = modpaths_by_name[minion_name]
                    actual_count = sum(persisted.get(modpath, 0) for modpath in modpaths)
                    if actual_count != expected_count:
                        pytest.fail(
                            f"ExpectRuntime.persistence mismatch for {minion_name}: "
                            f"expected {expected_count}, got {actual_count}."
                        )

            if resolutions:
                counters = target_checkpoint.metrics_counters
                if counters is None:
                    pytest.fail(
                        "ExpectRuntime.resolutions is unsupported with this Metrics snapshot strategy."
                    )

                for minion_name, expected_status_counts in resolutions.items():
                    if minion_name not in instance_ids_by_name:
                        pytest.fail(
                            f"ExpectRuntime.resolutions references unknown minion name: {minion_name!r}."
                        )
                    instance_ids = instance_ids_by_name[minion_name]
                    counts = {
                        "succeeded": sum(
                            int(s["value"])
                            for s in counters.get(MINION_WORKFLOW_SUCCEEDED_TOTAL, [])
                            if s["labels"].get(LABEL_MINION_INSTANCE_ID) in instance_ids
                        ),
                        "failed": sum(
                            int(s["value"])
                            for s in counters.get(MINION_WORKFLOW_FAILED_TOTAL, [])
                            if s["labels"].get(LABEL_MINION_INSTANCE_ID) in instance_ids
                        ),
                        "aborted": sum(
                            int(s["value"])
                            for s in counters.get(MINION_WORKFLOW_ABORTED_TOTAL, [])
                            if s["labels"].get(LABEL_MINION_INSTANCE_ID) in instance_ids
                        ),
                    }

                    for status, expected_count in expected_status_counts.items():
                        if status not in counts:
                            pytest.fail(
                                f"ExpectRuntime.resolutions has unknown status {status!r} for {minion_name}."
                            )
                        if not isinstance(expected_count, int) or expected_count < 0:
                            pytest.fail(
                                "ExpectRuntime.resolutions counts must be ints >= 0; "
                                f"got {minion_name}.{status}={expected_count!r}."
                            )
                        actual = counts[status]
                        if actual != expected_count:
                            pytest.fail(
                                f"ExpectRuntime.resolutions mismatch for {minion_name}.{status}: "
                                f"expected {expected_count}, got {actual}."
                            )

            if workflow_steps:
                if workflow_steps_mode not in ("at_least", "exact"):
                    pytest.fail(
                        "ExpectRuntime.workflow_steps_mode="
                        f"{workflow_steps_mode!r} is unsupported. Use 'at_least' or 'exact'."
                    )
                workflow_step_ids = target_checkpoint.workflow_step_started_ids_by_class
                if workflow_step_ids is None:
                    pytest.fail(
                        "ExpectRuntime.workflow_steps is unsupported without workflow step-id "
                        "checkpoint snapshots."
                    )

                spies = self._require_spies()
                class_key_by_modpath = {
                    modpath: f"{cls.__module__}.{cls.__name__}"
                    for modpath, cls in spies.minions.items()
                }

                for minion_name, expected_steps in workflow_steps.items():
                    if minion_name not in modpaths_by_name:
                        pytest.fail(
                            "ExpectRuntime.workflow_steps references unknown minion name: "
                            f"{minion_name!r}."
                        )
                    modpaths = modpaths_by_name[minion_name]
                    class_keys = [class_key_by_modpath[m] for m in modpaths if m in class_key_by_modpath]

                    for step_name, expected_count in expected_steps.items():
                        if not isinstance(expected_count, int) or expected_count < 0:
                            pytest.fail(
                                "ExpectRuntime.workflow_steps counts must be ints >= 0; "
                                f"got {minion_name}.{step_name}={expected_count!r}."
                            )
                        actual_count = sum(
                            len(workflow_step_ids.get(class_key, {}).get(step_name, ()))
                            for class_key in class_keys
                        )
                        if workflow_steps_mode == "at_least":
                            if actual_count < expected_count:
                                pytest.fail(
                                    "ExpectRuntime.workflow_steps mismatch for "
                                    f"{minion_name}.{step_name}: "
                                    f"expected >= {expected_count}, got {actual_count}."
                                )
                        else:
                            if actual_count != expected_count:
                                pytest.fail(
                                    "ExpectRuntime.workflow_steps mismatch for "
                                    f"{minion_name}.{step_name}: "
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
                    f"ExpectRuntime.at index {at} is out of range for {len(checkpoints)} checkpoint(s)."
                )
            return checkpoints[at]

        pytest.fail(
            f"ExpectRuntime.at={directive.at!r} is unsupported. Use 'latest' or a checkpoint index."
        )

    def _assert_minion_fanout_delivery(self) -> None:
        """Assert explicit per-minion fanout delivery from pipeline event targets.

        For each minion class with successful starts, each workflow step is expected
        to execute at least once per expected workflow.
        """
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)

        for m_cls, expected_workflows in expectations.expected_workflows_by_class.items():
            if expected_workflows < 0:
                pytest.fail(f"Invalid expected workflow count for {m_cls.__name__}: {expected_workflows}")

            actual_counts = m_cls.get_call_counts()

            for step_name in m_cls._mn_workflow_spec:  # type: ignore
                actual = actual_counts.get(step_name, 0)
                if actual < expected_workflows:
                    pytest.fail(
                        f"Fanout mismatch for {m_cls.__name__}.{step_name}: "
                        f"expected >= {expected_workflows} workflow calls from pipeline events, got {actual}."
                    )

    def _assert_pipeline_events(self) -> None:
        spies = self._require_spies()
        outcomes = self._compute_pipeline_attempt_outcomes()
        for modpath, p_cls in spies.pipelines.items():
            expected_events = self._plan.pipeline_event_targets.get(modpath)
            counts = p_cls.get_call_counts()

            attempts = max(
                outcomes.attempts_by_modpath.get(modpath, 0),
                spies.pipeline_start_attempt_counts.get(modpath, 0),
            )
            successes = outcomes.successes_by_modpath.get(modpath, 0)

            min_started = 1 if successes > 0 else 0
            max_started = attempts

            actual_inits = counts.get("__init__", 0)
            min_inits = 1 if successes > 0 else 0
            max_inits = attempts
            if actual_inits < min_inits or actual_inits > max_inits:
                pytest.fail(
                    f"{p_cls.__name__} __init__ mismatch: expected {min_inits}..{max_inits}, got {actual_inits}"
                )

            actual_startup = counts.get("startup", 0)
            if actual_startup < min_started or actual_startup > max_started:
                pytest.fail(
                    f"{p_cls.__name__} startup mismatch: expected {min_started}..{max_started}, got {actual_startup}"
                )

            actual_run = counts.get("run", 0)
            if actual_run < min_started or actual_run > actual_startup:
                pytest.fail(
                    f"{p_cls.__name__} run mismatch: expected {min_started}..{actual_startup}, got {actual_run}"
                )

            if self._result.seen_shutdown:
                actual_shutdown = counts.get("shutdown", 0)
                if actual_shutdown < 0 or actual_shutdown > actual_startup:
                    pytest.fail(
                        f"{p_cls.__name__} shutdown mismatch: expected 0..{actual_startup}, got {actual_shutdown}"
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
                        f"{p_cls.__name__} produce_event mismatch: expected 0 when no starts succeeded, got {actual}"
                    )

    def _compute_pipeline_attempt_outcomes(self) -> PipelineAttemptOutcomes:
        attempts_by_modpath: defaultdict[str, int] = defaultdict(int)
        successes_by_modpath: defaultdict[str, int] = defaultdict(int)
        for receipt in self._result.receipts:
            attempts_by_modpath[receipt.pipeline_modpath] += 1
            if receipt.success:
                successes_by_modpath[receipt.pipeline_modpath] += 1
        return PipelineAttemptOutcomes(
            attempts_by_modpath=attempts_by_modpath,
            successes_by_modpath=successes_by_modpath,
        )

    async def _pin_and_assert_calls(
        self,
        call_counts: dict[type[SpyMixin], dict[str, int]],
        allow_unlisted: set[type[SpyMixin]],
    ) -> list[Callable[[], None]]:
        try:
            return await asyncio.gather(*[
                cls.await_and_pin_call_counts(
                    expected=counts,
                    on_extra=_ExtraCallRecorder(self._result, cls),
                    allow_unlisted=(cls in allow_unlisted),
                    timeout=self._timeout,
                )
                for cls, counts in call_counts.items()
            ])
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

    def _assert_call_order(self, call_counts: dict[type[SpyMixin], dict[str, int]]) -> None:
        spies = self._require_spies()
        minion_start_counts = self._compute_minion_expectations(spies).minion_start_counts
        ss_tag = getattr(self._state_store, "_mspy_instance_tag", None)
        if ss_tag is not None:
            self._result.instance_tags[type(self._state_store)].add(ss_tag)

        for m_cls in spies.minions.values():
            if minion_start_counts.get(m_cls, 0) <= 0:
                continue
            expected_calls = call_counts.get(m_cls, {})
            workflow_steps = [
                name for name in m_cls._mn_workflow_spec  # type: ignore
                if expected_calls.get(name, 0) > 0
            ]
            for tag in self._result.instance_tags.get(m_cls, set()):
                names = _names_for_tag(m_cls, tag)
                order = ["__init__", "startup", "run", *workflow_steps]
                if "shutdown" in names:
                    order.append("shutdown")
                m_cls.assert_call_order_for_instance(tag, order)

        for modpath, p_cls in spies.pipelines.items():
            expected_events = self._plan.pipeline_event_targets.get(modpath)
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
            raise AssertionError("ScenarioRunner did not populate spies.")
        return self._result.spies
