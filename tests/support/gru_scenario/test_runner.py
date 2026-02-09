from pathlib import Path

import pytest

from tests.support.gru_scenario.directives import (
    Concurrent,
    GruShutdown,
    MinionStart,
    WaitWorkflows,
)
from tests.support.gru_scenario.plan import ScenarioPlan
from tests.support.gru_scenario.runner import ScenarioRunner


TESTS_DIR = Path(__file__).resolve().parents[2]


@pytest.mark.asyncio
async def test_runner_records_receipts_for_success_and_expected_failure(gru):
    config_path = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minion_simple",
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
    cfg1 = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    cfg2 = str(TESTS_DIR / "assets" / "minion_config_simple_2.toml")
    pipeline_modpath = "tests.assets.support.pipeline_wait_for_subs"
    reload_wait_for_subs_pipeline(expected_subs=2)

    directives = [
        Concurrent(
            MinionStart(
                minion="tests.assets.minion_simple",
                minion_config_path=cfg1,
                pipeline=pipeline_modpath,
            ),
            MinionStart(
                minion="tests.assets.minion_simple_resourced_2",
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
    config_path = str(TESTS_DIR / "assets" / "minion_config_simple_1.toml")
    pipeline_modpath = "tests.assets.pipeline_simple_single_event_1"

    directives = [
        MinionStart(
            minion="tests.assets.minion_simple",
            minion_config_path=config_path,
            pipeline=pipeline_modpath,
        ),
        MinionStart(
            minion="tests.assets.minion_simple",
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
