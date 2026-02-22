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

from .directives import MinionStart
from .plan import ScenarioPlan
from .runner import ScenarioRunResult, SpyRegistry


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
    minion_call_overrides: defaultdict[type[SpyMixin], dict[str, int]]


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
        self._assert_workflow_resolutions()
        self._assert_pipeline_events()

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
                "__init__": 1,
                "startup": starts,
                "run": starts,
                **{
                    name: expectations.expected_workflows_by_class.get(m_cls, 0)
                    for name in m_cls._mn_workflow_spec  # type: ignore
                },
            }
            overrides = expectations.minion_call_overrides.get(m_cls)
            if overrides:
                base.update(overrides)
            call_counts[m_cls] = base

        minion_starts = sum(expectations.minion_start_counts.values())
        workflows_started = sum(expectations.expected_workflows_by_class.values())
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
            "delete_context": workflows_started,
            "_delete_context": workflows_started,
        }
        if self._result.seen_shutdown:
            ss["shutdown"] = 1
        call_counts[type(self._state_store)] = ss

        return ExpectedCallCounts(call_counts=call_counts, allow_unlisted=allow_unlisted)

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
        minion_start_counts: defaultdict[type[SpyMixin], int] = defaultdict(int)
        expected_workflows_by_class: defaultdict[type[SpyMixin], int] = defaultdict(int)
        minion_call_overrides: defaultdict[type[SpyMixin], dict[str, int]] = defaultdict(dict)

        for receipt in self._result.receipts:
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

            run_spec = directive.expect
            overrides = run_spec.minion_call_overrides if run_spec else None
            if not overrides:
                continue
            for name, count in overrides.items():
                minion_call_overrides[m_cls][name] = (
                    minion_call_overrides[m_cls].get(name, 0) + count
                )

        return MinionExpectations(
            minion_start_counts=minion_start_counts,
            expected_workflows_by_class=expected_workflows_by_class,
            minion_call_overrides=minion_call_overrides,
        )

    def _assert_minion_fanout_delivery(self) -> None:
        """Assert explicit per-minion fanout delivery from pipeline event targets.

        For each minion class with successful starts, each workflow step is expected
        to execute exactly once per expected workflow unless explicitly overridden.
        """
        spies = self._require_spies()
        expectations = self._compute_minion_expectations(spies)

        for m_cls, expected_workflows in expectations.expected_workflows_by_class.items():
            if expected_workflows < 0:
                pytest.fail(f"Invalid expected workflow count for {m_cls.__name__}: {expected_workflows}")

            overrides = expectations.minion_call_overrides.get(m_cls, {})
            actual_counts = m_cls.get_call_counts()

            for step_name in m_cls._mn_workflow_spec:  # type: ignore
                if step_name in overrides:
                    continue
                actual = actual_counts.get(step_name, 0)
                if actual != expected_workflows:
                    pytest.fail(
                        f"Fanout mismatch for {m_cls.__name__}.{step_name}: "
                        f"expected {expected_workflows} workflow calls from pipeline events, got {actual}."
                    )

    def _assert_workflow_resolutions(self) -> None:
        counters = self._metrics.snapshot_counters()

        for receipt in self._result.receipts:
            if not receipt.success or receipt.instance_id is None:
                continue

            directive = self._plan.flat_directives[receipt.directive_index]
            if not isinstance(directive, MinionStart):
                continue
            if not directive.expect or not directive.expect.workflow_resolutions:
                continue

            expected = dict(directive.expect.workflow_resolutions)
            counts = {
                "succeeded": sum(
                    int(s["value"])
                    for s in counters.get(MINION_WORKFLOW_SUCCEEDED_TOTAL, [])
                    if s["labels"].get(LABEL_MINION_INSTANCE_ID) == receipt.instance_id
                ),
                "failed": sum(
                    int(s["value"])
                    for s in counters.get(MINION_WORKFLOW_FAILED_TOTAL, [])
                    if s["labels"].get(LABEL_MINION_INSTANCE_ID) == receipt.instance_id
                ),
                "aborted": sum(
                    int(s["value"])
                    for s in counters.get(MINION_WORKFLOW_ABORTED_TOTAL, [])
                    if s["labels"].get(LABEL_MINION_INSTANCE_ID) == receipt.instance_id
                ),
            }

            for status, expected_count in expected.items():
                if status not in counts:
                    pytest.fail(
                        f"Unknown workflow resolution '{status}' for {receipt.minion_modpath}"
                    )
                actual = counts[status]
                if actual != expected_count:
                    pytest.fail(
                        f"{receipt.minion_modpath} workflow {status} mismatch: "
                        f"expected {expected_count}, got {actual}"
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
                if successes > 0:
                    if actual not in (expected_events, expected_events + 1):
                        pytest.fail(
                            f"{p_cls.__name__} produce_event mismatch: "
                            f"expected {expected_events} or {expected_events + 1}, got {actual}"
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
