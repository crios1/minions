from tests.support.gru_scenario.directives import (
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    MinionStart,
    MinionStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
    WaitWorkflowStartsThen,
    WaitWorkflows,
    iter_directives_flat,
)


def test_iter_directives_flattens_concurrent():
    d1a = MinionStart(minion="m1", pipeline="p1")
    d1b = MinionStart(minion="m2", pipeline="p2")
    d2 = WaitWorkflows()
    d3a = MinionStop(name_or_instance_id="m1", expect_success=True)
    d3b = MinionStop(name_or_instance_id="m2", expect_success=True)
    d4 = GruShutdown()

    directives = [Concurrent(d1a, d1b), d2, Concurrent(d3a, d3b), d4]

    assert list(iter_directives_flat(directives)) == [d1a, d1b, d2, d3a, d3b, d4]


def test_wait_workflows_is_compatibility_alias_of_wait_workflow_completions():
    directive = WaitWorkflows(minion_names={"m1"})
    assert isinstance(directive, WaitWorkflowCompletions)
    assert directive.minion_names == {"m1"}


def test_iter_directives_flattens_wait_workflow_starts_then_wrapped_directive():
    d1 = MinionStart(minion="m1", pipeline="p1")
    d2 = MinionStop(name_or_instance_id="m1", expect_success=True)
    wrapped = WaitWorkflowStartsThen(expected={"m1": 1}, directive=d2)
    d3 = GruShutdown()

    directives = [d1, wrapped, d3]

    assert list(iter_directives_flat(directives)) == [d1, d2, d3]


def test_expect_runtime_defaults():
    directive = ExpectRuntime()
    assert directive.at == "latest"
    assert isinstance(directive.expect, RuntimeExpectSpec)
    assert directive.expect.persistence is None
