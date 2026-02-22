import asyncio
from pathlib import Path

import pytest

from tests.support.gru_scenario.directives import (
    Concurrent,
    GruShutdown,
    MinionStart,
    WaitWorkflows,
)
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import ScenarioRunResult, ScenarioRunner, ScenarioWaiter, SpyRegistry


TESTS_DIR = Path(__file__).resolve().parents[2]


@pytest.mark.asyncio
async def test_runner_records_receipts_for_success_and_expected_failure(gru):
    config_path = str(TESTS_DIR / "assets" / "config/minions/a.toml")
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
async def test_runner_concurrent_starts_capture_started_minions_and_instance_tags(
    gru, reload_wait_for_subs_pipeline
):
    cfg1 = str(TESTS_DIR / "assets" / "config/minions/a.toml")
    cfg2 = str(TESTS_DIR / "assets" / "config/minions/b.toml")
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
async def test_runner_wait_workflows_subset_handles_mixed_success_and_failure(gru):
    config_path = str(TESTS_DIR / "assets" / "config/minions/a.toml")
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
async def test_wait_minion_tasks_times_out_instead_of_blocking_indefinitely(gru):
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
async def test_wait_workflows_named_lookup_is_scenario_local_only(gru, monkeypatch):
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
