import pytest

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.assets.minions.two_steps.counter.basic import TwoStepMinion
from tests.assets.minions.two_steps.counter.resourced import TwoStepResourcedMinion
from tests.assets.pipelines.emit1.counter.emit_1 import Emit1Pipeline
from tests.support.gru_scenario.directives import (
    ExpectRuntime,
    MinionStart,
    RuntimeExpectSpec,
)
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import (
    ScenarioCheckpoint,
    ScenarioRunResult,
    SpyRegistry,
    StartReceipt,
)
from tests.support.gru_scenario.verify import ScenarioVerifier


def _mk_verifier(
    plan: ScenarioPlan,
    result: ScenarioRunResult,
    *,
    state_store: InMemoryStateStore | None = None,
) -> ScenarioVerifier:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = state_store or InMemoryStateStore(logger=logger)
    return ScenarioVerifier(
        plan,
        result,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
        per_verification_timeout=0.1,
    )


def test_compute_minion_expectations_accumulates_starts_from_successful_receipts():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-1", "n1", TwoStepMinion, True),
            StartReceipt(1, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-2", "n2", TwoStepMinion, True),
        ],
    )

    verifier = _mk_verifier(plan, result)
    expectations = verifier._compute_minion_expectations(spies)

    assert expectations.minion_start_counts[TwoStepMinion] == 2
    assert expectations.expected_workflows_by_class[TwoStepMinion] == 2


def test_build_expected_call_counts_state_store_formula_for_mixed_success_and_failure():
    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1", expect_success=False),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-ok", "ok", TwoStepMinion, True),
            StartReceipt(1, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-fail", "fail", TwoStepMinion, False),
        ],
        seen_shutdown=True,
    )

    verifier = _mk_verifier(plan, result)
    expected = verifier._build_expected_call_counts()
    state_store_counts = expected.call_counts[type(verifier._state_store)]

    # one successful start, two workflows, two steps per workflow
    assert state_store_counts["get_contexts_for_minion"] == 1
    assert state_store_counts["_get_contexts_for_minion"] == 1
    assert "get_all_contexts" not in state_store_counts
    assert state_store_counts["save_context"] == 6
    assert state_store_counts["_save_context"] == 4
    assert state_store_counts["delete_context"] == 2
    assert state_store_counts["_delete_context"] == 2
    assert state_store_counts["shutdown"] == 1


def test_build_expected_call_counts_scales_minion_init_with_successful_starts():
    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-a", "a", TwoStepMinion, True),
            StartReceipt(1, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-b", "b", TwoStepMinion, True),
        ],
    )

    verifier = _mk_verifier(plan, result)
    expected = verifier._build_expected_call_counts()
    minion_counts = expected.call_counts[TwoStepMinion]

    assert minion_counts["__init__"] == 2
    assert minion_counts["startup"] == 2
    assert minion_counts["run"] == 2


@pytest.mark.asyncio
async def test_build_expected_call_counts_does_not_require_get_all_for_overridden_context_lookup():
    class IndexedStateStore(InMemoryStateStore):
        async def get_contexts_for_minion(self, minion_modpath: str):
            return []

    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-ok", "ok", TwoStepMinion, True),
        ],
    )

    verifier = _mk_verifier(
        plan,
        result,
        state_store=IndexedStateStore(logger=InMemoryLogger()),
    )
    expected = verifier._build_expected_call_counts()
    state_store_counts = expected.call_counts[type(verifier._state_store)]

    assert state_store_counts["get_contexts_for_minion"] == 1
    assert state_store_counts["_get_contexts_for_minion"] == 1
    assert "get_all_contexts" not in state_store_counts


def test_assert_call_order_reports_extra_calls_with_details():
    plan = ScenarioPlan([], pipeline_event_counts={})
    result = ScenarioRunResult(
        spies=SpyRegistry(),
        extra_calls=[(TwoStepMinion, ("step_1",), {"count": 2})],
    )
    verifier = _mk_verifier(plan, result)
    # Scope this test to the extra-call diagnostics branch only.
    setattr(verifier._state_store, "_mspy_instance_tag", None)

    with pytest.raises(pytest.fail.Exception, match="Unexpected extra calls detected:"):
        verifier._assert_call_order(call_counts={})


def test_assert_state_store_read_call_bounds_rejects_excess_get_all_calls(monkeypatch):
    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-ok", "ok", TwoStepMinion, True),
        ],
    )
    verifier = _mk_verifier(plan, result)

    monkeypatch.setattr(type(verifier._state_store), "get_call_counts", classmethod(lambda cls: {"get_all_contexts": 2}))

    with pytest.raises(pytest.fail.Exception, match="get_all_contexts called more times than minion starts"):
        verifier._assert_state_store_read_call_bounds()


def test_assert_minion_fanout_delivery_proves_pipeline_event_delivery_to_steps(monkeypatch):
    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-ok", "ok", TwoStepMinion, True),
        ],
    )

    verifier = _mk_verifier(plan, result)
    monkeypatch.setattr(
        TwoStepMinion,
        "get_call_counts",
        classmethod(lambda cls: {"step_1": 2, "step_2": 2}),
    )

    verifier._assert_minion_fanout_delivery()


def test_assert_minion_fanout_delivery_reports_per_minion_mismatch_with_diagnostics(monkeypatch):
    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
        MinionStart(minion="tests.assets.minions.two_steps.counter.resourced", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 2},
    )
    spies = SpyRegistry(
        minions={
            "tests.assets.minions.two_steps.counter.basic": TwoStepMinion,
            "tests.assets.minions.two_steps.counter.resourced": TwoStepResourcedMinion,
        },
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-a", "a", TwoStepMinion, True),
            StartReceipt(1, "tests.assets.minions.two_steps.counter.resourced", "tests.assets.pipelines.emit1.counter.emit_1", "id-b", "b", TwoStepResourcedMinion, True),
        ],
    )

    verifier = _mk_verifier(plan, result)
    monkeypatch.setattr(
        TwoStepMinion,
        "get_call_counts",
        classmethod(lambda cls: {"step_1": 2, "step_2": 2}),
    )
    monkeypatch.setattr(
        TwoStepResourcedMinion,
        "get_call_counts",
        classmethod(lambda cls: {"step_1": 1, "step_2": 2}),
    )

    with pytest.raises(pytest.fail.Exception, match="Fanout mismatch for TwoStepResourcedMinion.step_1"):
        verifier._assert_minion_fanout_delivery()


def test_assert_pipeline_events_allows_restarted_pipeline_produce_event_totals(monkeypatch):
    directives = [
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
        MinionStart(minion="tests.assets.minions.two_steps.counter.basic", pipeline="tests.assets.pipelines.emit1.counter.emit_1"),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-a", "a", TwoStepMinion, True),
            StartReceipt(1, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "id-b", "b", TwoStepMinion, True),
        ],
    )
    verifier = _mk_verifier(plan, result)
    monkeypatch.setattr(
        Emit1Pipeline,
        "get_call_counts",
        classmethod(lambda cls: {"__init__": 2, "startup": 2, "run": 2, "produce_event": 3}),
    )

    verifier._assert_pipeline_events()


def test_assert_checkpoint_window_fanout_delivery_ignores_noop_wait_checkpoint():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(
                0,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "id-ok",
                "ok",
                TwoStepMinion,
                True,
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflows",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                minion_names=(),
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.basic.TwoStepMinion": {
                        "step_1": 0,
                        "step_2": 0,
                    }
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflows",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                minion_names=None,
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.basic.TwoStepMinion": {
                        "step_1": 1,
                        "step_2": 1,
                    }
                },
            ),
        ],
    )

    verifier = _mk_verifier(plan, result)
    verifier._assert_checkpoint_window_fanout_delivery()


def test_assert_checkpoint_window_fanout_delivery_handles_restart_phase_windows():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(
                0,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "id-pre-stop",
                "two-step-minion",
                TwoStepMinion,
                True,
            ),
            StartReceipt(
                1,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "id-post-restart",
                "two-step-minion",
                TwoStepMinion,
                True,
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_starts_then",
                directive_type="WaitWorkflowStartsThen",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                expected_starts={"two-step-minion": 1},
                wrapped_directive_type="MinionStop",
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.basic.TwoStepMinion": {
                        "step_1": 1,
                        "step_2": 0,
                    }
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflows",
                receipt_count=2,
                successful_receipt_count=2,
                seen_shutdown=False,
                minion_names=None,
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.basic.TwoStepMinion": {
                        "step_1": 2,
                        "step_2": 1,
                    }
                },
            ),
        ],
    )

    verifier = _mk_verifier(plan, result)
    verifier._assert_checkpoint_window_fanout_delivery()


def test_assert_runtime_expectations_persistence_at_latest_checkpoint():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(persistence={"two-step-minion": 1}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(
                0,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "id-ok",
                "two-step-minion",
                TwoStepMinion,
                True,
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_modpath={
                    "tests.assets.minions.two_steps.counter.basic": 1
                },
            ),
        ],
    )
    verifier = _mk_verifier(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_resolutions_at_latest_checkpoint():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={"two-step-minion": {"succeeded": 1, "failed": 0, "aborted": 0}}
            ),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(
                0,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "instance-1",
                "two-step-minion",
                TwoStepMinion,
                True,
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                metrics_counters={
                    "minion_workflow_succeeded_total": [
                        {"labels": {"minion_instance_id": "instance-1"}, "value": 1}
                    ],
                    "minion_workflow_failed_total": [
                        {"labels": {"minion_instance_id": "instance-1"}, "value": 0}
                    ],
                    "minion_workflow_aborted_total": [
                        {"labels": {"minion_instance_id": "instance-1"}, "value": 0}
                    ],
                },
            ),
        ],
    )
    verifier = _mk_verifier(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_persistence_at_checkpoint_index():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        ExpectRuntime(
            at=0,
            expect=RuntimeExpectSpec(persistence={"two-step-minion": 3}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(
                0,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "id-ok",
                "two-step-minion",
                TwoStepMinion,
                True,
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_modpath={
                    "tests.assets.minions.two_steps.counter.basic": 3
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_modpath={
                    "tests.assets.minions.two_steps.counter.basic": 99
                },
            ),
        ],
    )
    verifier = _mk_verifier(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_fails_for_out_of_range_checkpoint_index():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        ExpectRuntime(
            at=2,
            expect=RuntimeExpectSpec(persistence={"two-step-minion": 1}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(
                0,
                "tests.assets.minions.two_steps.counter.basic",
                "tests.assets.pipelines.emit1.counter.emit_1",
                "id-ok",
                "two-step-minion",
                TwoStepMinion,
                True,
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_modpath={
                    "tests.assets.minions.two_steps.counter.basic": 1
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_modpath={
                    "tests.assets.minions.two_steps.counter.basic": 1
                },
            ),
        ],
    )
    verifier = _mk_verifier(plan, result)

    with pytest.raises(
        pytest.fail.Exception,
        match="ExpectRuntime.at index 2 is out of range for 2 checkpoint\\(s\\)\\.",
    ):
        verifier._assert_runtime_expectations()
