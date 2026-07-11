from typing import Any, Protocol

import pytest

from minions._internal._domain.component_identity import get_component_id
from minions._internal._domain.gru import GruRuntimeStateSnapshot
from minions._internal._domain.minion_workflow_context import MinionWorkflowContext
from minions._internal._framework.metrics_constants import LABEL_ORCHESTRATION_ID
from minions._internal._framework.state_store import StoredWorkflowContext
from tests.assets.contexts.counter import CounterContext
from tests.assets.events.counter import CounterEvent
from tests.assets.minions.two_steps.counter.default import AssetMinion as TwoStepCounterMinion
from tests.assets.minions.two_steps.counter.identified_with_fixed_resource import (
    AssetMinion as IdentifiedMinion,
)
from tests.assets.minions.two_steps.counter.with_fixed_resource import (
    AssetMinion as FixedResourceMinion,
)
from tests.assets.pipelines.emit_one.counter.default import (
    AssetPipeline as EmitOneCounterPipeline,
)
from tests.assets.pipelines.emit_two.simple.with_subscriber_counts_one_then_two import (
    AssetPipeline as EmitTwoSimplePipeline,
)
from tests.assets.resources.fixed.default import AssetResource as FixedResource
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.state_store_inmemory import InMemoryStateStore
from tests.support.gru_scenario.directives import (
    AfterWorkflowStepStarts,
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    RuntimeExpectSpec,
)
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import (
    LifecycleObservation,
    OrchestrationStartReceipt,
    ScenarioCheckpoint,
    ScenarioRunResult,
    SpyRegistry,
)
from tests.support.gru_scenario.verify import ScenarioVerifier


def _stub_get_call_counts(counts: dict[str, int]) -> Any:
    def get_call_counts(cls: type[object]) -> dict[str, int]:
        return dict(counts)

    return classmethod(get_call_counts)


class VerifierFactory(Protocol):
    def __call__(
        self,
        plan: ScenarioPlan,
        result: ScenarioRunResult,
        *,
        state_store: InMemoryStateStore | None = None,
    ) -> ScenarioVerifier: ...


@pytest.fixture
def verifier_factory(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
    state_store: InMemoryStateStore,
) -> VerifierFactory:
    default_state_store = state_store

    def _mk_verifier(
        plan: ScenarioPlan,
        result: ScenarioRunResult,
        *,
        state_store: InMemoryStateStore | None = None,
    ) -> ScenarioVerifier:
        return ScenarioVerifier(
            plan,
            result,
            logger=logger,
            metrics=metrics,
            state_store=state_store or default_state_store,
            per_verification_timeout=0.1,
        )

    return _mk_verifier


def test_verifier_require_spies_invariant_message_is_actionable(verifier_factory: VerifierFactory):
    verifier = verifier_factory(
        ScenarioPlan([], pipeline_event_counts={}),
        ScenarioRunResult(spies=None, receipts=[]),
    )
    with pytest.raises(
        AssertionError,
        match=r"internal invariant violated: result\.spies is None.*ScenarioRunner\.run\(\)",
    ):
        verifier._require_spies()


def test_require_workflow_spec_rejects_missing_spec(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(TwoStepCounterMinion, "_mn_workflow_spec", None)

    with pytest.raises(
        pytest.fail.Exception,
        match="workflow spec missing; Minion setup is deferred or incomplete",
    ):
        ScenarioVerifier._require_workflow_spec(TwoStepCounterMinion)


def test_require_workflow_spec_rejects_empty_spec(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(TwoStepCounterMinion, "_mn_workflow_spec", ())

    with pytest.raises(
        pytest.fail.Exception,
        match="workflow spec is empty; a valid Minion must define at least one @minion_step",
    ):
        ScenarioVerifier._require_workflow_spec(TwoStepCounterMinion)


def test_assert_metrics_label_contract_reports_recorded_mismatches(
    verifier_factory: VerifierFactory,
):
    verifier = verifier_factory(
        ScenarioPlan([], pipeline_event_counts={}),
        ScenarioRunResult(spies=SpyRegistry(), receipts=[]),
    )
    metric = verifier._metrics.create_metric("unknown_metric_total", [], "counter")
    metric.labels().inc()

    with pytest.raises(pytest.fail.Exception, match="Metrics label contract mismatch"):
        verifier._assert_metrics_label_contract()

    assert isinstance(verifier._metrics, InMemoryMetrics)

    verifier._metrics.clear_metric_label_emissions()


def test_compute_minion_expectations_accumulates_starts_from_successful_receipts(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-2",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    expectations = verifier._compute_minion_expectations(spies)

    assert expectations.minion_start_counts[TwoStepCounterMinion] == 2
    assert expectations.expected_workflows_by_class[TwoStepCounterMinion] == 2


def test_build_expected_call_counts_state_store_formula_for_mixed_success_and_failure(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
            expect_success=False,
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-fail",
                minion_cls=TwoStepCounterMinion,
                success=False,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        seen_shutdown=True,
    )

    verifier = verifier_factory(plan, result)
    expected = verifier._build_expected_call_counts()
    state_store_counts = expected.call_counts[type(verifier._state_store)]

    # one successful start -> two workflows, with one initial save and two
    # per-step saves per workflow
    assert state_store_counts["get_contexts_for_orchestration"] == 1
    assert state_store_counts["_mn_get_contexts_for_orchestration"] == 1
    assert state_store_counts["_mn_get_decoded_contexts_for_orchestration"] == 1
    assert state_store_counts["_mn_decode_stored_contexts"] == 1
    assert "get_all_contexts" not in state_store_counts
    assert state_store_counts["save_context"] == 6
    assert state_store_counts["_mn_save_context"] == 6
    assert state_store_counts["_mn_serialize_and_save_context"] == 6
    assert state_store_counts["delete_context"] == 2
    assert state_store_counts["_mn_delete_context"] == 2
    assert state_store_counts["shutdown"] == 1


def test_build_expected_call_counts_scales_minion_init_with_successful_starts(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-a",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-b",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    expected = verifier._build_expected_call_counts()
    minion_counts = expected.call_counts[TwoStepCounterMinion]

    assert minion_counts["__init__"] == 2
    assert minion_counts["startup"] == 2
    assert minion_counts["run"] == 2


def test_build_expected_call_counts_keeps_pipelines_out_of_exact_pinning(
    verifier_factory: VerifierFactory,
):
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    minion_ref = "tests.assets.minions.two_steps.counter.with_fixed_resource"
    plan = ScenarioPlan(
        [OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)],
        pipeline_event_counts={pipeline_ref: 1},
    )
    spies = SpyRegistry(
        minions={minion_ref: FixedResourceMinion},
        pipelines={pipeline_ref: EmitOneCounterPipeline},
        resources={FixedResource},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id="id-ok",
                minion_cls=FixedResourceMinion,
                success=True,
                orchestration_id=None,
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
        ],
    )

    expected = verifier_factory(plan, result)._build_expected_call_counts()

    assert EmitOneCounterPipeline not in expected.call_counts
    assert EmitOneCounterPipeline not in expected.allow_unlisted
    assert FixedResource in expected.call_counts
    assert FixedResource in expected.allow_unlisted


@pytest.mark.asyncio
async def test_build_expected_call_counts_does_not_require_get_all_for_overridden_context_lookup(
    verifier_factory: VerifierFactory,
    logger: InMemoryLogger,
):
    class IndexedStateStore(InMemoryStateStore):
        async def get_contexts_for_orchestration(
            self,
            orchestration_id: str,
        ) -> list[StoredWorkflowContext]:
            return []

    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )

    verifier = verifier_factory(
        plan,
        result,
        state_store=IndexedStateStore(logger=logger),
    )
    expected = verifier._build_expected_call_counts()
    state_store_counts = expected.call_counts[type(verifier._state_store)]

    assert state_store_counts["get_contexts_for_orchestration"] == 1
    assert state_store_counts["_mn_get_contexts_for_orchestration"] == 1
    assert state_store_counts["_mn_get_decoded_contexts_for_orchestration"] == 1
    assert state_store_counts["_mn_decode_stored_contexts"] == 1
    assert "get_all_contexts" not in state_store_counts


def test_assert_call_order_reports_extra_calls_with_details(verifier_factory: VerifierFactory):
    plan = ScenarioPlan([], pipeline_event_counts={})
    result = ScenarioRunResult(
        spies=SpyRegistry(),
        extra_calls=[(TwoStepCounterMinion, ("step_1",), {"count": 2})],
    )
    verifier = verifier_factory(plan, result)
    # Scope this test to the extra-call diagnostics branch only.
    setattr(verifier._state_store, "_mspy_instance_tag", None)

    with pytest.raises(pytest.fail.Exception, match="Unexpected extra calls detected:"):
        verifier._assert_call_order(call_counts={})


def test_assert_state_store_read_call_bounds_rejects_excess_get_all_calls(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )
    verifier = verifier_factory(plan, result)

    monkeypatch.setattr(
        type(verifier._state_store),
        "get_call_counts",
        _stub_get_call_counts({"get_all_contexts": 2}),
    )

    with pytest.raises(
        pytest.fail.Exception, match="get_all_contexts called more times than allowed"
    ):
        verifier._assert_state_store_read_call_bounds()


def test_assert_minion_fanout_delivery_proves_pipeline_event_delivery_to_steps(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        TwoStepCounterMinion,
        "get_call_counts",
        _stub_get_call_counts({"step_1": 2, "step_2": 2}),
    )

    verifier._assert_minion_fanout_delivery()


def test_assert_minion_fanout_delivery_reports_per_minion_mismatch_with_diagnostics(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.with_fixed_resource",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 2},
    )
    spies = SpyRegistry(
        minions={
            "tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion,
            "tests.assets.minions.two_steps.counter.with_fixed_resource": FixedResourceMinion,
        },
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-a",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.with_fixed_resource",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-b",
                minion_cls=FixedResourceMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.with_fixed_resource",
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        TwoStepCounterMinion,
        "get_call_counts",
        _stub_get_call_counts({"step_1": 2, "step_2": 2}),
    )
    monkeypatch.setattr(
        FixedResourceMinion,
        "get_call_counts",
        _stub_get_call_counts({"step_1": 1, "step_2": 2}),
    )

    with pytest.raises(
        pytest.fail.Exception,
        match=(
            r"Fanout mismatch for tests\.assets\.minions\.two_steps\.counter\."
            r"with_fixed_resource:AssetMinion\.step_1"
        ),
    ):
        verifier._assert_minion_fanout_delivery()


def test_assert_minion_fanout_delivery_allows_plus_one_per_start_tolerance(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        TwoStepCounterMinion,
        "get_call_counts",
        _stub_get_call_counts({"step_1": 3, "step_2": 3}),
    )

    verifier._assert_minion_fanout_delivery()


def test_assert_minion_fanout_delivery_rejects_overage_beyond_plus_one(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        TwoStepCounterMinion,
        "get_call_counts",
        _stub_get_call_counts({"step_1": 4, "step_2": 3}),
    )

    with pytest.raises(
        pytest.fail.Exception,
        match=(
            r"Fanout mismatch for tests\.assets\.minions\.two_steps\.counter\."
            r"default:AssetMinion\.step_1: expected 2\.\.3 workflow calls from "
            r"pipeline events, got 4"
        ),
    ):
        verifier._assert_minion_fanout_delivery()


def test_assert_pipeline_events_uses_exact_singleton_activation_counts_by_default(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-a",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-b",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitOneCounterPipeline,
        "get_call_counts",
        _stub_get_call_counts({"__init__": 1, "startup": 1, "run": 1, "produce_event": 1}),
    )

    verifier._assert_pipeline_events()


def test_assert_pipeline_events_uses_pipeline_total_events_for_produce_calls(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_two.simple.with_subscriber_counts_one_then_two"
    minion_ref = "tests.assets.minions.two_steps.simple.default"
    plan = ScenarioPlan(
        [OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)],
        pipeline_event_counts={pipeline_ref: 1},
    )
    spies = SpyRegistry(
        minions={minion_ref: TwoStepCounterMinion},
        pipelines={pipeline_ref: EmitTwoSimplePipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitTwoSimplePipeline,
        "get_call_counts",
        _stub_get_call_counts({"__init__": 1, "startup": 1, "run": 1, "produce_event": 2}),
    )

    verifier._assert_pipeline_events()


def test_assert_pipeline_events_keeps_concurrent_starts_exact(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    minion_ref = "tests.assets.minions.two_steps.counter.default"
    start_a = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    start_b = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    plan = ScenarioPlan(
        [Concurrent(start_a, start_b)],
        pipeline_event_counts={pipeline_ref: 1},
    )
    spies = SpyRegistry(
        minions={minion_ref: TwoStepCounterMinion},
        pipelines={pipeline_ref: EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id="id-a",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id="id-b",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitOneCounterPipeline,
        "get_call_counts",
        _stub_get_call_counts({"__init__": 1, "startup": 1, "run": 1, "produce_event": 1}),
    )

    verifier._assert_pipeline_events()


def test_assert_pipeline_events_keeps_lifecycle_exact_for_stop_boundary(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    minion_ref = "tests.assets.minions.two_steps.counter.default"
    start = OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref)
    plan = ScenarioPlan(
        [
            start,
            AfterWorkflowStepStarts(
                expected={start: {"step_1": 1}},
                directive=OrchestrationStop(id=start, expect_success=True),
            ),
            GruShutdown(expect_success=True),
        ],
        pipeline_event_counts={pipeline_ref: 1},
    )
    spies = SpyRegistry(
        minions={minion_ref: TwoStepCounterMinion},
        pipelines={pipeline_ref: EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="orch-ok",
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
        ],
        seen_shutdown=True,
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitOneCounterPipeline,
        "get_call_counts",
        _stub_get_call_counts(
            {"__init__": 1, "startup": 1, "run": 1, "shutdown": 1, "produce_event": 2}
        ),
    )

    verifier._assert_pipeline_events()


def test_assert_pipeline_events_ignores_failed_start_when_pipeline_is_active(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    minion_ref = "tests.assets.minions.two_steps.counter.default"
    directives = [
        OrchestrationStart(pipeline=pipeline_ref, minion=minion_ref),
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            expect_success=False,
        ),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_ref: 1})
    spies = SpyRegistry(
        minions={minion_ref: TwoStepCounterMinion},
        pipelines={pipeline_ref: EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="orch-ok",
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id=None,
                minion_cls=None,
                success=False,
                orchestration_id=None,
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
        ],
        seen_shutdown=True,
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitOneCounterPipeline,
        "get_call_counts",
        _stub_get_call_counts(
            {"__init__": 1, "startup": 1, "run": 1, "shutdown": 1, "produce_event": 1}
        ),
    )

    verifier._assert_pipeline_events()


def test_assert_pipeline_events_falls_back_for_failed_start_before_activation(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    minion_ref = "tests.assets.minions.two_steps.counter.default"
    directives = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            expect_success=False,
        ),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(directives, pipeline_event_counts={})
    spies = SpyRegistry(
        minions={minion_ref: TwoStepCounterMinion},
        pipelines={pipeline_ref: EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id=None,
                minion_cls=None,
                success=False,
                orchestration_id=None,
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
            ),
        ],
        seen_shutdown=True,
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitOneCounterPipeline,
        "get_call_counts",
        _stub_get_call_counts({}),
    )

    verifier._assert_pipeline_events()


def test_assert_pipeline_events_handles_event_type_failure_before_activation(
    verifier_factory: VerifierFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    pipeline_ref = "tests.assets.pipelines.emit_one.counter.default"
    minion_ref = "tests.assets.minions.two_steps.simple.default"
    directives = [
        OrchestrationStart(
            pipeline=pipeline_ref,
            minion=minion_ref,
            expect_success=False,
        ),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(directives, pipeline_event_counts={})
    spies = SpyRegistry(
        minions={minion_ref: TwoStepCounterMinion},
        pipelines={pipeline_ref: EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_ref,
                pipeline_module_path=pipeline_ref,
                instance_id=None,
                minion_cls=None,
                success=False,
                orchestration_id="orch-failed",
                pipeline_id=pipeline_ref,
                minion_id=minion_ref,
                failure_reason=(
                    "Incompatible minion and pipeline event types: "
                    "pipeline_emits=CounterEvent; minion_expects=SimpleEvent"
                ),
                failure_category="event_type_mismatch",
            ),
        ],
        seen_shutdown=True,
    )
    verifier = verifier_factory(plan, result)
    monkeypatch.setattr(
        EmitOneCounterPipeline,
        "get_call_counts",
        _stub_get_call_counts({"__init__": 1}),
    )

    verifier._assert_pipeline_events()


def test_assert_workflow_step_start_events_are_monotonic_allows_resume_replay_at_same_step(
    verifier_factory: VerifierFactory,
):
    plan = ScenarioPlan(
        [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.counter.default",
                minion="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                workflow_step_start_events_by_minion_id={
                    minion_id: {
                        "workflow-1": (
                            (0, "step_1"),
                            (1, "step_2"),
                            (1, "step_2"),
                        ),
                    },
                },
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    verifier._assert_workflow_step_start_events_are_monotonic()


def test_assert_workflow_step_start_events_are_monotonic_rejects_regression(
    verifier_factory: VerifierFactory,
):
    plan = ScenarioPlan(
        [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.counter.default",
                minion="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                workflow_step_start_events_by_minion_id={
                    minion_id: {
                        "workflow-1": (
                            (0, "step_1"),
                            (1, "step_2"),
                            (0, "step_1"),
                        ),
                    },
                },
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    with pytest.raises(pytest.fail.Exception, match="not monotonic"):
        verifier._assert_workflow_step_start_events_are_monotonic()


def test_assert_workflow_step_start_events_are_monotonic_rejects_step_name_index_mismatch(
    verifier_factory: VerifierFactory,
):
    plan = ScenarioPlan(
        [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.counter.default",
                minion="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                workflow_step_start_events_by_minion_id={
                    minion_id: {
                        "workflow-1": ((0, "step_2"),),
                    },
                },
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    with pytest.raises(pytest.fail.Exception, match="step_name/index mismatch"):
        verifier._assert_workflow_step_start_events_are_monotonic()


def test_assert_checkpoint_window_workflow_step_progression_ignores_noop_wait_checkpoint(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                orchestration_directive_indexes=(),
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": 0,
                        "step_2": 0,
                    }
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                orchestration_directive_indexes=None,
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": 1,
                        "step_2": 1,
                    }
                },
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    verifier._assert_checkpoint_window_workflow_step_progression()


def test_assert_checkpoint_window_workflow_step_progression_handles_restart_phase_windows(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-pre-stop",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-post-restart",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_step_starts_then",
                directive_type="AfterWorkflowStepStarts",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                expected_step_starts={0: {"step_1": 1}},
                wrapped_directive_type="OrchestrationStop",
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": 1,
                        "step_2": 0,
                    }
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=2,
                successful_receipt_count=2,
                seen_shutdown=False,
                orchestration_directive_indexes=None,
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": 2,
                        "step_2": 1,
                    }
                },
            ),
        ],
    )

    verifier = verifier_factory(plan, result)
    verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_exact_fails_on_overage(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="durable-two-step-minion",
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
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": 2,
                        "step_2": 2,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    "durable-two-step-minion": {
                        "step_1": ("workflow-1", "workflow-2"),
                        "step_2": ("workflow-1", "workflow-2"),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)

    with pytest.raises(pytest.fail.Exception, match="expected workflow-id delta 1, got 2"):
        verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_supports_mixed_modes_per_window(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-2",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                orchestration_directive_indexes=None,
                workflow_steps_mode="at_least",
                spy_call_counts={
                    class_key: {
                        "step_1": 2,  # tolerated overage for first window
                        "step_2": 2,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        "step_1": ("workflow-1", "workflow-2"),
                        "step_2": ("workflow-1", "workflow-2"),
                    }
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=2,
                successful_receipt_count=2,
                seen_shutdown=False,
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    class_key: {
                        "step_1": 3,  # exact +1 delta over prior checkpoint
                        "step_2": 3,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        "step_1": ("workflow-1", "workflow-2", "workflow-3"),
                        "step_2": ("workflow-1", "workflow-2", "workflow-3"),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_exact_with_workflow_ids_allows_call_count_overage(  # noqa: E501
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    class_key: {
                        "step_1": 2,  # overage tolerated when workflow-id exactness is available
                        "step_2": 2,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        "step_1": ("workflow-1",),
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_exact_with_workflow_ids_rejects_overage_beyond_start_tolerance(  # noqa: E501
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    class_key: {
                        "step_1": 3,  # expected 1, max tolerated 2 (start_count=1)
                        "step_2": 2,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        "step_1": ("workflow-1",),
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    with pytest.raises(
        pytest.fail.Exception,
        match=r"Checkpoint workflow-step progression mismatch.*expected call-count delta "
        r"1\.\.2, got 3",
    ):
        verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_exact_multi_instance_overlap_passes_with_workflow_id_exactness(  # noqa: E501
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-2",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=2,
                successful_receipt_count=2,
                seen_shutdown=False,
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    class_key: {
                        "step_1": 3,  # overage tolerated with workflow-id evidence
                        "step_2": 3,
                    }
                },
                spy_call_counts_by_instance={
                    class_key: {
                        1: {"step_1": 2, "step_2": 2},
                        2: {"step_1": 1, "step_2": 1},
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        "step_1": ("workflow-1", "workflow-2"),
                        "step_2": ("workflow-1", "workflow-2"),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_exact_multi_instance_overlap_reports_instance_deltas_on_workflow_id_mismatch(  # noqa: E501
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
            OrchestrationStartReceipt(
                directive_index=1,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-2",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_completions",
                directive_type="WaitWorkflowCompletions",
                receipt_count=2,
                successful_receipt_count=2,
                seen_shutdown=False,
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    class_key: {
                        "step_1": 3,
                        "step_2": 3,
                    }
                },
                spy_call_counts_by_instance={
                    class_key: {
                        1: {"step_1": 2, "step_2": 2},
                        2: {"step_1": 1, "step_2": 1},
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        # exact mode expects 2 new workflow ids for this window
                        "step_1": ("workflow-1",),
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    with pytest.raises(
        pytest.fail.Exception,
        match=r"expected workflow-id delta 2, got 1\..*Per-instance deltas: \{1: 2, 2: 1\}",
    ):
        verifier._assert_checkpoint_window_workflow_step_progression()


def test_checkpoint_window_workflow_step_progression_exact_prioritizes_workflow_id_mismatch_when_both_fail(  # noqa: E501
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    class_key = "tests.assets.minions.two_steps.counter.default.AssetMinion"
    minion_id = "tests.assets.minions.two_steps.counter.default"
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                orchestration_directive_indexes=None,
                workflow_steps_mode="exact",
                spy_call_counts={
                    class_key: {
                        "step_1": 3,  # expected 1..2
                        "step_2": 1,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    minion_id: {
                        "step_1": tuple(),  # expected workflow-id delta 1
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    with pytest.raises(
        pytest.fail.Exception,
        match=(
            r"Checkpoint workflow-id progression mismatch.*expected workflow-id "
            r"delta 1, got 0\..*Call-count delta: 3 \(expected 1\.\.2\)"
        ),
    ):
        verifier._assert_checkpoint_window_workflow_step_progression()


def test_instance_step_deltas_reports_non_zero_deltas_sorted(verifier_factory: VerifierFactory):
    verifier = verifier_factory(
        ScenarioPlan([], pipeline_event_counts={}),
        ScenarioRunResult(spies=SpyRegistry(), receipts=[]),
    )
    deltas = verifier._instance_step_deltas(
        step_name="step_1",
        curr_by_instance={
            3: {"step_1": 4},
            1: {"step_1": 2},
            2: {"step_1": 1},
        },
        prev_by_instance={
            1: {"step_1": 1},
            2: {"step_1": 1},
            4: {"step_1": 5},
        },
    )

    assert deltas == {1: 1, 3: 4, 4: -5}


def test_workflow_id_delta_count_computes_new_ids_only(verifier_factory: VerifierFactory):
    verifier = verifier_factory(
        ScenarioPlan([], pipeline_event_counts={}),
        ScenarioRunResult(spies=SpyRegistry(), receipts=[]),
    )

    assert verifier._workflow_id_delta_count(curr=("w1", "w2", "w3"), prev=("w2",)) == 2
    assert verifier._workflow_id_delta_count(curr=("w1",), prev=("w1", "w2")) == 0


def test_checkpoint_window_fanout_fails_when_workflow_id_delta_below_expected(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 2},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id=None,
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                orchestration_directive_indexes=None,
                spy_call_counts={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": 2,
                        "step_2": 2,
                    }
                },
                workflow_step_started_ids_by_minion_id={
                    "tests.assets.minions.two_steps.counter.default.AssetMinion": {
                        "step_1": ("workflow-1",),
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)

    with pytest.raises(pytest.fail.Exception, match="Checkpoint workflow-id progression mismatch"):
        verifier._assert_checkpoint_window_workflow_step_progression()


def test_assert_runtime_expectations_persistence_at_latest_checkpoint(
    verifier_factory: VerifierFactory,
):
    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(persistence={start: 1}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="durable-two-step-minion",
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
                persisted_contexts_by_orchestration_id={"instance-1": 1},
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_lifecycle_tracking_reports_untracked_successful_start(
    verifier_factory: VerifierFactory,
):
    plan = ScenarioPlan(
        [
            OrchestrationStart(
                pipeline="tests.assets.pipelines.emit_one.counter.default",
                minion="tests.assets.minions.two_steps.counter.default",
            )
        ],
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="minion-instance-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="orchestration-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        lifecycle_observations=[
            LifecycleObservation(
                directive_type=OrchestrationStart,
                receipt_count=1,
                active_orchestration_start_indexes=frozenset({0}),
                seen_shutdown=False,
                gru_runtime_state=GruRuntimeStateSnapshot(
                    minion_instances=frozenset({"minion-instance-1"}),
                    orchestrations=frozenset(),
                    minion_tasks=frozenset({"minion-instance-1"}),
                    pipelines=frozenset({"tests.assets.pipelines.emit_one.counter.default"}),
                    pipeline_tasks=frozenset({"tests.assets.pipelines.emit_one.counter.default"}),
                    resources=frozenset(),
                    resource_tasks=frozenset(),
                    minion_instance_by_orchestration={},
                    pipeline_by_minion_instance={},
                    resources_by_minion_instance={},
                    resources_by_pipeline={},
                    resource_dependencies_by_dependent_resource={},
                    resource_dependents_by_dependency_resource={},
                    resource_reference_counts={},
                ),
            ),
        ],
    )

    with pytest.raises(
        pytest.fail.Exception,
        match=(
            "Gru lifecycle tracking mismatch: "
            "directive=OrchestrationStart, observation_index=0, "
            "state=orchestrations"
        ),
    ):
        verifier_factory(plan, result)._assert_lifecycle_tracking()


def test_assert_lifecycle_tracking_reports_resource_refcount_mismatch(
    verifier_factory: VerifierFactory,
):
    minion_id = "tests.assets.minions.two_steps.counter.with_fixed_resource"
    pipeline_id = "tests.assets.pipelines.emit_one.counter.with_fixed_resource"
    resource_id = "tests.assets.resources.fixed.default.AssetResource"
    plan = ScenarioPlan(
        [OrchestrationStart(pipeline=pipeline_id, minion=minion_id)],
        pipeline_event_counts={pipeline_id: 1},
    )
    result = ScenarioRunResult(
        spies=SpyRegistry(
            minions={minion_id: TwoStepCounterMinion},
            pipelines={pipeline_id: EmitOneCounterPipeline},
            resources_by_minion_id={minion_id: frozenset({resource_id})},
            resources_by_pipeline={pipeline_id: frozenset({resource_id})},
        ),
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=minion_id,
                pipeline_module_path=pipeline_id,
                instance_id="minion-instance-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="orchestration-1",
                pipeline_id=pipeline_id,
                minion_id=minion_id,
            ),
        ],
        lifecycle_observations=[
            LifecycleObservation(
                directive_type=OrchestrationStart,
                receipt_count=1,
                active_orchestration_start_indexes=frozenset({0}),
                seen_shutdown=False,
                gru_runtime_state=GruRuntimeStateSnapshot(
                    minion_instances=frozenset({"minion-instance-1"}),
                    orchestrations=frozenset({"orchestration-1"}),
                    minion_tasks=frozenset({"minion-instance-1"}),
                    pipelines=frozenset({pipeline_id}),
                    pipeline_tasks=frozenset({pipeline_id}),
                    resources=frozenset({resource_id}),
                    resource_tasks=frozenset({resource_id}),
                    minion_instance_by_orchestration={"orchestration-1": "minion-instance-1"},
                    pipeline_by_minion_instance={"minion-instance-1": pipeline_id},
                    resources_by_minion_instance={"minion-instance-1": frozenset({resource_id})},
                    resources_by_pipeline={pipeline_id: frozenset({resource_id})},
                    resource_dependencies_by_dependent_resource={},
                    resource_dependents_by_dependency_resource={},
                    resource_reference_counts={resource_id: 3},
                ),
            ),
        ],
    )

    with pytest.raises(
        pytest.fail.Exception,
        match="state=resource_reference_counts",
    ):
        verifier_factory(plan, result)._assert_lifecycle_tracking()


def test_assert_persisted_context_integrity_accepts_matching_snapshot(
    verifier_factory: VerifierFactory,
):
    directives = [
        OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="dummy-orchestration-id",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="durable-two-step-minion",
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
                persisted_context_snapshots_by_minion_id={
                    "durable-two-step-minion": (
                        MinionWorkflowContext(
                            orchestration_id="dummy-orchestration-id",
                            workflow_id="workflow-1",
                            event=CounterEvent(seq=1),
                            context=CounterContext(),
                            next_step_index=0,
                        ),
                    )
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_persisted_context_integrity()


def test_assert_runtime_expectations_resolutions_at_latest_checkpoint(
    verifier_factory: VerifierFactory,
):
    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                resolutions={start: {"succeeded": 1, "failed": 0, "aborted": 0}}
            ),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="instance-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                        {"labels": {LABEL_ORCHESTRATION_ID: "instance-1"}, "value": 1}
                    ],
                    "minion_workflow_failed_total": [
                        {"labels": {LABEL_ORCHESTRATION_ID: "instance-1"}, "value": 0}
                    ],
                    "minion_workflow_aborted_total": [
                        {"labels": {LABEL_ORCHESTRATION_ID: "instance-1"}, "value": 0}
                    ],
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_workflow_steps_exact_at_latest_checkpoint(
    verifier_factory: VerifierFactory,
):
    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                workflow_steps={start: {"step_1": 1, "step_2": 1}},
                workflow_steps_mode="exact",
            ),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="instance-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                workflow_step_started_ids_by_orchestration_id={
                    "instance-1": {
                        "step_1": ("workflow-1",),
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_workflow_steps_at_least_allows_overage(
    verifier_factory: VerifierFactory,
):
    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        ExpectRuntime(
            expect=RuntimeExpectSpec(
                workflow_steps={start: {"step_1": 1}},
                workflow_steps_mode="at_least",
            ),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="instance-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                workflow_step_started_ids_by_orchestration_id={
                    "instance-1": {
                        "step_1": ("workflow-1", "workflow-2"),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_persistence_at_checkpoint_index(
    verifier_factory: VerifierFactory,
):
    identified_counter_minion_id = get_component_id(IdentifiedMinion)
    assert identified_counter_minion_id is not None, "IdentifiedMinion must have a component id"

    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.identified_with_fixed_resource",
        ),
        ExpectRuntime(
            at=0,
            expect=RuntimeExpectSpec(persistence={start: 3}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={
            "tests.assets.minions.two_steps.counter.identified_with_fixed_resource": (
                IdentifiedMinion
            ),
        },
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path=(
                    "tests.assets.minions.two_steps.counter.identified_with_fixed_resource"
                ),
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=IdentifiedMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id=identified_counter_minion_id,
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
                persisted_contexts_by_orchestration_id={"instance-1": 3},
            ),
            ScenarioCheckpoint(
                order=1,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_orchestration_id={"instance-1": 99},
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_workflow_steps_exact_at_checkpoint_index(
    verifier_factory: VerifierFactory,
):
    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        ExpectRuntime(
            at=0,
            expect=RuntimeExpectSpec(
                workflow_steps={start: {"step_1": 1, "step_2": 0}},
                workflow_steps_mode="exact",
            ),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="instance-1",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
            ),
        ],
        checkpoints=[
            ScenarioCheckpoint(
                order=0,
                kind="wait_workflow_step_starts_then",
                directive_type="AfterWorkflowStepStarts",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                expected_step_starts={0: {"step_1": 1}},
                wrapped_directive_type="OrchestrationStop",
                workflow_step_started_ids_by_orchestration_id={
                    "instance-1": {
                        "step_1": ("workflow-1",),
                        "step_2": (),
                    }
                },
            ),
            ScenarioCheckpoint(
                order=1,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                workflow_step_started_ids_by_orchestration_id={
                    "instance-1": {
                        "step_1": ("workflow-1",),
                        "step_2": ("workflow-1",),
                    }
                },
            ),
        ],
    )
    verifier = verifier_factory(plan, result)
    verifier._assert_runtime_expectations()


def test_assert_runtime_expectations_fails_for_out_of_range_checkpoint_index(
    verifier_factory: VerifierFactory,
):
    directives = [
        start := OrchestrationStart(
            pipeline="tests.assets.pipelines.emit_one.counter.default",
            minion="tests.assets.minions.two_steps.counter.default",
        ),
        ExpectRuntime(
            at=2,
            expect=RuntimeExpectSpec(persistence={start: 1}),
        ),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit_one.counter.default": 1},
    )
    spies = SpyRegistry(
        minions={"tests.assets.minions.two_steps.counter.default": TwoStepCounterMinion},
        pipelines={"tests.assets.pipelines.emit_one.counter.default": EmitOneCounterPipeline},
    )
    result = ScenarioRunResult(
        spies=spies,
        receipts=[
            OrchestrationStartReceipt(
                directive_index=0,
                minion_module_path="tests.assets.minions.two_steps.counter.default",
                pipeline_module_path="tests.assets.pipelines.emit_one.counter.default",
                instance_id="id-ok",
                minion_cls=TwoStepCounterMinion,
                success=True,
                orchestration_id="instance-1",
                pipeline_id="tests.assets.pipelines.emit_one.counter.default",
                minion_id="tests.assets.minions.two_steps.counter.default",
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
                persisted_contexts_by_orchestration_id={"instance-1": 1},
            ),
            ScenarioCheckpoint(
                order=1,
                kind="expect_runtime",
                directive_type="ExpectRuntime",
                receipt_count=1,
                successful_receipt_count=1,
                seen_shutdown=False,
                persisted_contexts_by_orchestration_id={"instance-1": 1},
            ),
        ],
    )
    verifier = verifier_factory(plan, result)

    with pytest.raises(
        pytest.fail.Exception,
        match="ExpectRuntime.at index 2 is out of range for 2 checkpoint\\(s\\)\\.",
    ):
        verifier._assert_runtime_expectations()
