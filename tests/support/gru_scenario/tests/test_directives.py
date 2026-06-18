import pytest

from tests.support.gru_scenario.directives import (
    AfterWorkflowStepStarts,
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
    iter_directives_flat,
)


def test_iter_directives_flattens_concurrent():
    d1a = OrchestrationStart(pipeline="p1", minion="m1")
    d1b = OrchestrationStart(pipeline="p2", minion="m2")
    d2 = WaitWorkflowCompletions()
    d3a = OrchestrationStop(id=d1a, expect_success=True)
    d3b = OrchestrationStop(id=d1b, expect_success=True)
    d4 = GruShutdown()

    directives = [Concurrent(d1a, d1b), d2, Concurrent(d3a, d3b), d4]

    assert list(iter_directives_flat(directives)) == [d1a, d1b, d2, d3a, d3b, d4]


def test_wait_workflow_completions_accepts_orchestrations_and_mode():
    start = OrchestrationStart(pipeline="p1", minion="m1")
    directive = WaitWorkflowCompletions(
        orchestrations=(start,),
        workflow_steps_mode="exact",
    )
    assert isinstance(directive, WaitWorkflowCompletions)
    assert directive.orchestrations == (start,)
    assert directive.workflow_steps_mode == "exact"


def test_orchestration_start_accepts_class_inputs_and_inline_config():
    from tests.assets.minions.two_steps.simple.configured import ConfiguredSimpleMinion
    from tests.assets.pipelines.simple.simple_event.single_event_1 import SimpleSingleEventPipeline1
    from tests.assets.support.minion_spied_configed import AssetMinionConfig

    config = AssetMinionConfig(name="inline")
    directive = OrchestrationStart(
        pipeline=SimpleSingleEventPipeline1,
        minion=ConfiguredSimpleMinion,
        minion_config=config,
    )

    assert directive.minion_module_path == "tests.assets.minions.two_steps.simple.configured"
    assert directive.pipeline_module_path == (
        "tests.assets.pipelines.simple.simple_event.single_event_1"
    )
    assert directive.as_kwargs() == {
        "minion": ConfiguredSimpleMinion,
        "pipeline": SimpleSingleEventPipeline1,
        "minion_config_path": None,
        "minion_config": config,
    }


def test_orchestration_starts_use_identity_with_unhashable_inline_configs():
    start_a = OrchestrationStart(
        pipeline="p1",
        minion="m1",
        minion_config={"values": []},
    )
    start_b = OrchestrationStart(
        pipeline="p1",
        minion="m1",
        minion_config={"values": []},
    )

    workflow_steps = {
        start_a: {"step_1": 1},
        start_b: {"step_1": 2},
    }

    assert start_a != start_b
    assert workflow_steps[start_a] == {"step_1": 1}
    assert workflow_steps[start_b] == {"step_1": 2}


def test_iter_directives_flattens_wait_workflow_step_starts_then_wrapped_directive():
    d1 = OrchestrationStart(pipeline="p1", minion="m1")
    d2 = OrchestrationStop(id=d1, expect_success=True)
    wrapped = AfterWorkflowStepStarts(expected={d1: {"step_2": 1}}, directive=d2)
    d3 = GruShutdown()

    directives = [d1, wrapped, d3]

    assert list(iter_directives_flat(directives)) == [d1, d2, d3]


def test_expect_runtime_defaults():
    directive = ExpectRuntime()
    assert directive.at == "latest"
    assert isinstance(directive.expect, RuntimeExpectSpec)
    assert directive.expect.persistence is None


@pytest.mark.parametrize("mode", ["strictly", ""])
def test_wait_workflow_completions_rejects_invalid_workflow_steps_mode(
    mode: str,
):
    with pytest.raises(
        ValueError,
        match="WaitWorkflowCompletions.workflow_steps_mode must be",
    ):
        WaitWorkflowCompletions(workflow_steps_mode=mode)  # type: ignore[arg-type]


@pytest.mark.parametrize("mode", ["strictly", ""])
def test_runtime_expect_spec_rejects_invalid_workflow_steps_mode(
    mode: str,
):
    with pytest.raises(
        ValueError,
        match="RuntimeExpectSpec.workflow_steps_mode must be",
    ):
        RuntimeExpectSpec(workflow_steps_mode=mode)  # type: ignore[arg-type]
