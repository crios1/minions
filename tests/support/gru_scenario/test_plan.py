import pytest

from tests.support.gru_scenario.directives import (
    Concurrent,
    GruShutdown,
    MinionStart,
    MinionStop,
    WaitWorkflows,
)
from tests.support.gru_scenario.plan import ScenarioPlan


def test_scenario_plan_flattens_and_indexes_directives():
    d1a = MinionStart(minion="m1", pipeline="p1")
    d1b = MinionStart(minion="m2", pipeline="p2")
    d1c = MinionStart(minion="m3", pipeline="p3")
    d1 = Concurrent(d1a, d1b, d1c)
    d2 = MinionStop(name_or_instance_id="m1", expect_success=True)
    d3 = WaitWorkflows()
    d4 = GruShutdown()

    plan = ScenarioPlan([d1, d2, d3, d4], pipeline_event_counts={"p1": 1, "p2": 1, "p3": 1})

    assert plan.directives == [d1, d2, d3, d4]
    assert plan.flat_directives == [d1a, d1b, d1c, d2, d3, d4]
    for idx, directive in enumerate(plan.flat_directives):
        assert plan.directive_index(directive) == idx


def test_scenario_plan_copies_pipeline_event_counts():
    counts = {"p1": 2}
    d1 = MinionStart(minion="m1", pipeline="p1")

    plan = ScenarioPlan([d1, GruShutdown()], pipeline_event_counts=counts)

    counts["p1"] = 99
    assert plan.pipeline_event_targets == {"p1": 2}


def test_scenario_plan_uses_identity_for_index_lookup():
    d1 = MinionStop(name_or_instance_id="m1", expect_success=True)
    d2 = MinionStop(name_or_instance_id="m1", expect_success=True)

    plan = ScenarioPlan([d1], pipeline_event_counts={})

    with pytest.raises(KeyError):
        plan.directive_index(d2)


def test_scenario_plan_accepts_empty_directives():
    plan = ScenarioPlan([], pipeline_event_counts={})

    assert plan.directives == []
    assert plan.flat_directives == []
    assert plan.pipeline_event_targets == {}


def test_scenario_plan_accepts_stop_and_shutdown_only_directives():
    d1 = MinionStop(name_or_instance_id="missing", expect_success=False)
    d2 = GruShutdown(expect_success=True)

    plan = ScenarioPlan([d1, d2], pipeline_event_counts={})

    assert plan.flat_directives == [d1, d2]
    assert plan.pipeline_event_targets == {}


def test_scenario_plan_accepts_zero_pipeline_event_count_for_started_pipeline():
    d1 = MinionStart(minion="m1", pipeline="p1")

    plan = ScenarioPlan([d1], pipeline_event_counts={"p1": 0})

    assert plan.pipeline_event_targets == {"p1": 0}


def test_scenario_plan_flattens_nested_concurrent_and_indexes_all_children():
    d1 = MinionStart(minion="m1", pipeline="p1")
    d2 = MinionStart(minion="m2", pipeline="p2")
    d3 = MinionStop(name_or_instance_id="m1", expect_success=True)
    d4 = MinionStop(name_or_instance_id="m2", expect_success=True)
    d5 = GruShutdown(expect_success=True)

    nested = Concurrent(
        d1,
        Concurrent(
            d2,
            Concurrent(d3),
        ),
    )

    plan = ScenarioPlan([nested, d4, d5], pipeline_event_counts={"p1": 1, "p2": 1})

    assert plan.flat_directives == [d1, d2, d3, d4, d5]
    for idx, directive in enumerate(plan.flat_directives):
        assert plan.directive_index(directive) == idx


def test_scenario_plan_raises_when_missing_pipeline_event_count_for_started_pipeline():
    d1 = MinionStart(minion="m1", pipeline="p1")

    with pytest.raises(ValueError, match="Missing pipeline_event_counts entries"):
        ScenarioPlan([d1], pipeline_event_counts={})


def test_scenario_plan_raises_when_pipeline_event_count_has_unused_entry():
    d1 = MinionStart(minion="m1", pipeline="p1")

    with pytest.raises(ValueError, match="pipelines not started in directives"):
        ScenarioPlan([d1], pipeline_event_counts={"p1": 1, "p2": 1})


def test_scenario_plan_raises_when_pipeline_event_count_is_negative():
    d1 = MinionStart(minion="m1", pipeline="p1")

    with pytest.raises(ValueError, match="must be >= 0"):
        ScenarioPlan([d1], pipeline_event_counts={"p1": -1})


def test_scenario_plan_raises_when_pipeline_event_count_is_not_int():
    d1 = MinionStart(minion="m1", pipeline="p1")

    with pytest.raises(ValueError, match="must be an int"):
        ScenarioPlan([d1], pipeline_event_counts={"p1": 1.5})  # type: ignore[arg-type]


def test_scenario_plan_raises_when_reusing_same_directive_instance():
    shared = MinionStart(minion="m1", pipeline="p1")

    with pytest.raises(ValueError, match="Directives must be unique instances"):
        ScenarioPlan([Concurrent(shared, shared)], pipeline_event_counts={"p1": 1})


def test_scenario_plan_copies_directives_input_list():
    directives = [MinionStop(name_or_instance_id="m1", expect_success=True)]
    plan = ScenarioPlan(directives, pipeline_event_counts={})

    directives.append(GruShutdown(expect_success=True))

    assert len(plan.directives) == 1
    assert len(plan.flat_directives) == 1
