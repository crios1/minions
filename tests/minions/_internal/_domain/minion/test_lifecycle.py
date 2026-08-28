import pytest

from minions import Minion, minion_step
from minions._internal._domain.exceptions import MinionsError
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger


@pytest.mark.asyncio
async def test_startup_failure_context_includes_minion_identity(
    logger: InMemoryLogger,
):
    class FailingStartupMinion(Minion[EmptyEvent, EmptyContext]):
        async def startup(self):
            raise RuntimeError("boom")

        @minion_step
        async def step_1(self): ...

    m = FailingStartupMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=NoOpStateStore(),
        metrics=NoOpMetrics(),
        logger=logger,
        minion_id="dummy-minion-id",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )

    with pytest.raises(MinionsError) as exc_info:
        await m._mn_startup()

    assert exc_info.value.context["minion_id"] == "dummy-minion-id"
    assert exc_info.value.context["minion_instance_id"] == "dummy-minion-instance-id"
    assert exc_info.value.context["minion_config_id"] == ""
    assert exc_info.value.context["minion_module_path"] == "dummy-minion-module-path"
