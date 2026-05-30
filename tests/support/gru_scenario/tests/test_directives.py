from tests.support.gru_scenario.directives import (
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    OrchestrationStart,
    OrchestrationStop,
    RuntimeExpectSpec,
    WaitWorkflowCompletions,
    AfterWorkflowStepStarts,
    iter_directives_flat,
)


def test_iter_directives_flattens_concurrent():
    d1a = OrchestrationStart(minion="m1", pipeline="p1")
    d1b = OrchestrationStart(minion="m2", pipeline="p2")
    d2 = WaitWorkflowCompletions()
    d3a = OrchestrationStop(id=d1a, expect_success=True)
    d3b = OrchestrationStop(id=d1b, expect_success=True)
    d4 = GruShutdown()

    directives = [Concurrent(d1a, d1b), d2, Concurrent(d3a, d3b), d4]

    assert list(iter_directives_flat(directives)) == [d1a, d1b, d2, d3a, d3b, d4]


def test_wait_workflow_completions_accepts_minion_names_and_mode():
    directive = WaitWorkflowCompletions(minion_names={"m1"}, workflow_steps_mode="exact")
    assert isinstance(directive, WaitWorkflowCompletions)
    assert directive.minion_names == {"m1"}
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

    assert directive.minion_modpath == "tests.assets.minions.two_steps.simple.configured"
    assert directive.pipeline_modpath == "tests.assets.pipelines.simple.simple_event.single_event_1"
    assert directive.as_kwargs() == {
        "minion": ConfiguredSimpleMinion,
        "pipeline": SimpleSingleEventPipeline1,
        "minion_config_path": None,
        "minion_config": config,
    }


def test_iter_directives_flattens_wait_workflow_step_starts_then_wrapped_directive():
    d1 = OrchestrationStart(minion="m1", pipeline="p1")
    d2 = OrchestrationStop(id=d1, expect_success=True)
    wrapped = AfterWorkflowStepStarts(expected={"m1": {"step_2": 1}}, directive=d2)
    d3 = GruShutdown()

    directives = [d1, wrapped, d3]

    assert list(iter_directives_flat(directives)) == [d1, d2, d3]


def test_expect_runtime_defaults():
    directive = ExpectRuntime()
    assert directive.at == "latest"
    assert isinstance(directive.expect, RuntimeExpectSpec)
    assert directive.expect.persistence is None
