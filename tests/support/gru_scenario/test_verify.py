import pytest

from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.assets.minions.two_steps.counter.basic import TwoStepMinion
from tests.assets.pipelines.emit1.counter.emit_1 import Emit1Pipeline
from tests.support.gru_scenario.directives import MinionRunSpec, MinionStart
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import ScenarioRunResult, SpyRegistry, StartReceipt
from tests.support.gru_scenario.verify import ScenarioVerifier


def _mk_verifier(plan: ScenarioPlan, result: ScenarioRunResult) -> ScenarioVerifier:
    logger = InMemoryLogger()
    metrics = InMemoryMetrics()
    state_store = InMemoryStateStore(logger=logger)
    return ScenarioVerifier(
        plan,
        result,
        logger=logger,
        metrics=metrics,
        state_store=state_store,
        per_verification_timeout=0.1,
    )


def test_compute_minion_expectations_accumulates_overrides_from_successful_receipts():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
            expect=MinionRunSpec(minion_call_overrides={"step_1": 1}),
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
            expect=MinionRunSpec(minion_call_overrides={"step_1": 2}),
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
    assert expectations.minion_call_overrides[TwoStepMinion]["step_1"] == 3


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
    assert state_store_counts["load_all_contexts"] == 1
    assert state_store_counts["_load_all_contexts"] == 1
    assert state_store_counts["save_context"] == 6
    assert state_store_counts["_save_context"] == 4
    assert state_store_counts["delete_context"] == 2
    assert state_store_counts["_delete_context"] == 2
    assert state_store_counts["shutdown"] == 1


def test_assert_workflow_resolutions_uses_instance_id_filtering():
    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.counter.basic",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
            expect=MinionRunSpec(workflow_resolutions={"failed": 1}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 0},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.basic": TwoStepMinion},
        pipelines={"tests.assets.pipelines.emit1.counter.emit_1": Emit1Pipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            StartReceipt(0, "tests.assets.minions.two_steps.counter.basic", "tests.assets.pipelines.emit1.counter.emit_1", "target-id", "n", TwoStepMinion, True),
        ],
    )

    verifier = _mk_verifier(plan, result)
    verifier._metrics.snapshot_counters = lambda: {
        "minion_workflow_failed_total": [
            {"labels": {"minion_instance_id": "target-id"}, "value": 1},
            {"labels": {"minion_instance_id": "other-id"}, "value": 999},
        ]
    }

    verifier._assert_workflow_resolutions()


def test_assert_call_order_reports_extra_calls_with_details():
    plan = ScenarioPlan([], pipeline_event_counts={})
    result = ScenarioRunResult(
        spies=SpyRegistry(),
        extra_calls=[(TwoStepMinion, ("step_1",), {"count": 2})],
    )
    verifier = _mk_verifier(plan, result)
    # Scope this test to the extra-call diagnostics branch only.
    verifier._state_store._mspy_instance_tag = None

    with pytest.raises(pytest.fail.Exception, match="Unexpected extra calls detected:"):
        verifier._assert_call_order(call_counts={})
