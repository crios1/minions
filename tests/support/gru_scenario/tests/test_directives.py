from tests.support.gru_scenario.directives import (
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    MinionStart,
    MinionStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
    AfterWorkflowStarts,
    iter_directives_flat,
)


def test_iter_directives_flattens_concurrent():
    d1a = MinionStart(minion="m1", pipeline="p1")
    d1b = MinionStart(minion="m2", pipeline="p2")
    d2 = WaitWorkflowCompletions()
    d3a = MinionStop(name_or_instance_id="m1", expect_success=True)
    d3b = MinionStop(name_or_instance_id="m2", expect_success=True)
    d4 = GruShutdown()

    directives = [Concurrent(d1a, d1b), d2, Concurrent(d3a, d3b), d4]

    assert list(iter_directives_flat(directives)) == [d1a, d1b, d2, d3a, d3b, d4]


def test_wait_workflow_completions_accepts_minion_names_and_mode():
    directive = WaitWorkflowCompletions(minion_names={"m1"}, workflow_steps_mode="exact")
    assert isinstance(directive, WaitWorkflowCompletions)
    assert directive.minion_names == {"m1"}
    assert directive.workflow_steps_mode == "exact"


def test_iter_directives_flattens_wait_workflow_starts_then_wrapped_directive():
    d1 = MinionStart(minion="m1", pipeline="p1")
    d2 = MinionStop(name_or_instance_id="m1", expect_success=True)
    wrapped = AfterWorkflowStarts(expected={"m1": 1}, directive=d2)
    d3 = GruShutdown()

    directives = [d1, wrapped, d3]

    assert list(iter_directives_flat(directives)) == [d1, d2, d3]


def test_expect_runtime_defaults():
    directive = ExpectRuntime()
    assert directive.at == "latest"
    assert isinstance(directive.expect, RuntimeExpectSpec)
    assert directive.expect.persistence is None
