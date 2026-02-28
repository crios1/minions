import asyncio
import pytest

from tests.support.gru_scenario.directives import (
    Concurrent,
    ExpectRuntime,
    GruShutdown,
    MinionStart,
    MinionStop,
    RuntimeExpectSpec,
    WaitWorkflowStartsThen,
    WaitWorkflows,
)
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import ScenarioRunResult, ScenarioRunner, ScenarioWaiter, SpyRegistry



@pytest.mark.asyncio
async def test_runner_records_receipts_for_success_and_expected_failure(gru, tests_dir):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect_success=False,
        ),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_modpath: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 2
    assert result.receipts[0].directive_index == 0
    assert result.receipts[0].success is True
    assert result.receipts[0].instance_id is not None
    assert result.receipts[0].resolved_name == "simple-minion"

    assert result.receipts[1].directive_index == 1
    assert result.receipts[1].success is False

    assert result.seen_shutdown is True


@pytest.mark.asyncio
async def test_runner_concurrent_starts_capture_started_minions_and_instance_tags(gru, tests_dir, reload_wait_for_subs_pipeline
):
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")
    cfg2 = str(tests_dir / "assets" / "config/minions/b.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.basic",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion="tests.assets.minions.two_steps.simple.resourced_2",
                minion_config_path=cfg2,
                pipeline=pipeline_modpath,
            ),
        ),
        WaitWorkflows(),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_modpath: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 2
    assert all(r.success for r in result.receipts)
    assert {r.directive_index for r in result.receipts} == {0, 1}
    assert len(result.started_minions) == 2

    started_names = {m._mn_name for m in result.started_minions}
    assert started_names == {"simple-minion", "simple-resourced-minion-2"}

    assert result.spies is not None
    pipeline_cls = result.spies.pipelines[pipeline_modpath]
    assert len(result.instance_tags.get(pipeline_cls, set())) >= 1

    assert any(len(result.instance_tags.get(type(m), set())) >= 1 for m in result.started_minions)
    assert any(len(result.instance_tags.get(r_cls, set())) >= 1 for r_cls in result.spies.resources)


@pytest.mark.asyncio
async def test_runner_wait_workflows_subset_handles_mixed_success_and_failure(gru, tests_dir):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
            expect_success=False,
        ),
        WaitWorkflows(minion_names={"simple-minion"}),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_modpath: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert len(result.receipts) == 2
    assert sum(1 for r in result.receipts if r.success) == 1
    assert sum(1 for r in result.receipts if not r.success) == 1
    assert len(result.started_minions) == 1


@pytest.mark.asyncio
async def test_wait_minion_tasks_times_out_instead_of_blocking_indefinitely(gru, tests_dir):
    class _DummyMinion:
        def __init__(self):
            self._mn_tasks_lock = asyncio.Lock()
            self._mn_tasks: set[asyncio.Task] = set()
            self._mn_aux_tasks: set[asyncio.Task] = set()

    plan = ScenarioPlan([], pipeline_event_counts={})
    waiter = ScenarioWaiter(
        plan,
        ScenarioRunner(gru, plan, per_verification_timeout=0.01)._insp,
        timeout=0.01,
        spies=SpyRegistry(),
        result=ScenarioRunResult(),
    )

    dummy = _DummyMinion()
    task = asyncio.create_task(asyncio.sleep(60), name="never-finishes")
    dummy._mn_tasks.add(task)

    try:
        with pytest.raises(pytest.fail.Exception, match="WaitWorkflows timed out"):
            await waiter._wait_minion_tasks({dummy})  # type: ignore[arg-type]
    finally:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task


@pytest.mark.asyncio
async def test_wait_workflows_named_lookup_is_scenario_local_only(gru, tests_dir, monkeypatch):
    plan = ScenarioPlan([], pipeline_event_counts={})
    runner = ScenarioRunner(gru, plan, per_verification_timeout=0.1)
    waiter = ScenarioWaiter(
        plan,
        runner._insp,
        timeout=0.1,
        spies=SpyRegistry(),
        result=ScenarioRunResult(),
    )

    def _should_not_be_called(*_args, **_kwargs):
        raise AssertionError("runtime-global lookup must not be used by WaitWorkflows")

    monkeypatch.setattr(waiter._insp, "get_minions_by_name", _should_not_be_called)

    with pytest.raises(pytest.fail.Exception, match="Unknown minion names in WaitWorkflows"):
        await waiter.wait(minion_names={"not-started-in-scenario"})


@pytest.mark.asyncio
async def test_runner_wait_workflow_starts_then_rejects_unsupported_wrapped_directive(gru):
    directives = [
        WaitWorkflowStartsThen(
            expected={"simple-minion": 1},
            directive=GruShutdown(expect_success=True),
        ),
    ]
    plan = ScenarioPlan(directives, pipeline_event_counts={})
    runner = ScenarioRunner(gru, plan, per_verification_timeout=0.1)

    with pytest.raises(pytest.fail.Exception, match="supports wrapping MinionStop only"):
        await runner.run()


@pytest.mark.asyncio
async def test_runner_wait_workflow_starts_then_rejects_unknown_names(gru):
    directives = [
        WaitWorkflowStartsThen(
            expected={"missing": 1},
            directive=MinionStop(name_or_instance_id="missing", expect_success=False),
        ),
    ]
    plan = ScenarioPlan(directives, pipeline_event_counts={})
    runner = ScenarioRunner(gru, plan, per_verification_timeout=0.1)

    with pytest.raises(pytest.fail.Exception, match="Unknown minion names in WaitWorkflowStartsThen.expected"):
        await runner.run()


@pytest.mark.asyncio
async def test_runner_records_checkpoints_for_wait_workflow_completions_and_shutdown(gru, tests_dir):
    config_path = str(tests_dir / "assets" / "config/minions/a.toml")
    pipeline_modpath = "tests.assets.pipelines.simple.simple_event.single_event_1"

    directives = [
        MinionStart(
            minion="tests.assets.minions.two_steps.simple.basic",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
        ),
        WaitWorkflows(),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(directives, pipeline_event_counts={pipeline_modpath: 1})
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    assert [cp.kind for cp in result.checkpoints] == [
        "wait_workflow_completions",
        "gru_shutdown",
    ]
    assert [cp.order for cp in result.checkpoints] == [0, 1]
    assert result.checkpoints[0].directive_type == "WaitWorkflows"
    assert result.checkpoints[0].minion_names is None
    assert result.checkpoints[1].directive_type == "GruShutdown"
    assert result.checkpoints[1].seen_shutdown is True


@pytest.mark.asyncio
async def test_runner_records_checkpoint_for_wait_workflow_starts_then(gru):
    directives = [
        MinionStart(
            minion="tests.assets.minions.failure.abort_step",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        WaitWorkflowStartsThen(
            expected={"abort-step-minion": 1},
            directive=MinionStop(name_or_instance_id="abort-step-minion", expect_success=True),
        ),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    checkpoints = result.checkpoints
    assert [cp.kind for cp in checkpoints] == ["wait_workflow_starts_then", "gru_shutdown"]
    assert checkpoints[0].directive_type == "WaitWorkflowStartsThen"
    assert checkpoints[0].expected_starts == {"abort-step-minion": 1}
    assert checkpoints[0].wrapped_directive_type == "MinionStop"


@pytest.mark.asyncio
async def test_runner_restart_flow_checkpoints_separate_pre_stop_and_post_restart_windows(gru, tests_dir):
    pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"
    minion_modpath = "tests.assets.minions.two_steps.counter.basic"
    cfg1 = str(tests_dir / "assets" / "config/minions/a.toml")

    directives = [
        MinionStart(minion=minion_modpath, minion_config_path=cfg1, pipeline=pipeline_modpath),
        WaitWorkflowStartsThen(
            expected={"two-step-minion": 1},
            directive=MinionStop(name_or_instance_id="two-step-minion", expect_success=True),
        ),
        MinionStart(minion=minion_modpath, minion_config_path=cfg1, pipeline=pipeline_modpath),
        WaitWorkflows(),
        GruShutdown(expect_success=True),
    ]

    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    checkpoints = result.checkpoints
    assert [cp.kind for cp in checkpoints] == [
        "wait_workflow_starts_then",
        "wait_workflow_completions",
        "gru_shutdown",
    ]

    pre_stop_cp = checkpoints[0]
    post_restart_cp = checkpoints[1]

    assert pre_stop_cp.receipt_count == 1
    assert post_restart_cp.receipt_count == 2

    key = "tests.assets.minions.two_steps.counter.basic.TwoStepMinion"
    assert pre_stop_cp.spy_call_counts is not None
    assert post_restart_cp.spy_call_counts is not None

    pre_step1 = pre_stop_cp.spy_call_counts.get(key, {}).get("step_1", 0)
    post_step1 = post_restart_cp.spy_call_counts.get(key, {}).get("step_1", 0)
    assert post_step1 >= pre_step1 + 1


@pytest.mark.asyncio
async def test_runner_restart_same_composite_key_after_stop_succeeds(gru):
    pipeline_modpath = "tests.assets.pipelines.emit1.counter.emit_1"
    minion_modpath = "tests.assets.minions.failure.abort_step"

    directives = [
        MinionStart(minion=minion_modpath, pipeline=pipeline_modpath),
        WaitWorkflowStartsThen(
            expected={"abort-step-minion": 1},
            directive=MinionStop(name_or_instance_id="abort-step-minion", expect_success=True),
        ),
        MinionStart(minion=minion_modpath, pipeline=pipeline_modpath, expect_success=True),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={pipeline_modpath: 1},
    )
    await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()


@pytest.mark.asyncio
async def test_runner_records_expect_runtime_checkpoint_with_persistence_snapshot(gru):
    directives = [
        MinionStart(
            minion="tests.assets.minions.failure.slow_step",
            pipeline="tests.assets.pipelines.emit1.counter.emit_1",
        ),
        WaitWorkflowStartsThen(
            expected={"slow-step-minion": 1},
            directive=MinionStop(name_or_instance_id="slow-step-minion", expect_success=True),
        ),
        ExpectRuntime(expect=RuntimeExpectSpec(persistence={"slow-step-minion": 1})),
        GruShutdown(expect_success=True),
    ]
    plan = ScenarioPlan(
        directives,
        pipeline_event_counts={"tests.assets.pipelines.emit1.counter.emit_1": 1},
    )
    result = await ScenarioRunner(gru, plan, per_verification_timeout=5.0).run()

    expect_cps = [cp for cp in result.checkpoints if cp.kind == "expect_runtime"]
    assert len(expect_cps) == 1
    persisted = expect_cps[0].persisted_contexts_by_modpath
    assert persisted is not None
    assert persisted.get("tests.assets.minions.failure.slow_step", 0) >= 1
