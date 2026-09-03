import pytest

from minions import Minion, Pipeline, Resource, minion_step
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import (
    LABEL_ORCHESTRATION_ID,
    LABEL_RESOURCE,
    LABEL_RESOURCE_CALLER,
    LABEL_RESOURCE_CALLER_KIND,
    LABEL_RESOURCE_METHOD,
    RESOURCE_SERVES_TOTAL,
)
from minions._internal._framework.metrics_noop import NoOpMetrics
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.contexts.empty import EmptyContext
from tests.assets.events.empty import EmptyEvent
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics
from tests.assets.support.minion_noop import NoOpMinion


@pytest.mark.asyncio
async def test_pipeline_runtime_metric_labels_match_contract(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    class SuccessPipeline(Pipeline[EmptyEvent]):
        async def produce_event(self) -> EmptyEvent:
            return EmptyEvent()

    class ErrorPipeline(Pipeline[EmptyEvent]):
        async def produce_event(self) -> EmptyEvent:
            raise RuntimeError("boom")

    success_pipeline = SuccessPipeline(
        "contract-pipeline",
        "tests.metrics_contract.SuccessPipeline",
        metrics,
        logger,
    )
    minion = NoOpMinion(
        minion_instance_id="dummy-minion-instance-id",
        orchestration_id="dummy-orchestration-id",
        minion_module_path="dummy-minion-module-path",
        config_path=None,
        state_store=NoOpStateStore(),
        metrics=NoOpMetrics(),
        logger=NoOpLogger(),
        minion_id="dummy-minion-id",
        minion_config_id="dummy-minion-config-id",
        pipeline_id="contract-pipeline",
    )
    minion._mn_mark_running()
    await success_pipeline._mn_subscribe(minion)

    await success_pipeline._mn_produce_and_fan_out_event()
    await minion._mn_wait_until_workflows_idle()

    error_pipeline = ErrorPipeline(
        "contract-error-pipeline",
        "tests.metrics_contract.ErrorPipeline",
        metrics,
        logger,
    )
    await error_pipeline._mn_produce_and_fan_out_event()

    metrics.assert_metric_label_observations_match_contract()


@pytest.mark.asyncio
async def test_resource_runtime_metric_labels_preserve_transitive_callers(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    class TransitiveResource(Resource):
        async def read_value(self) -> int:
            return 1

    class PipelineResource(Resource):
        transitive_resource: TransitiveResource

        async def load_value(self) -> int:
            return await self.transitive_resource.read_value()

    transitive_resource = TransitiveResource(
        logger,
        metrics,
        "tests.metrics_contract.TransitiveResource",
        resource_id="contract-transitive-resource",
    )
    pipeline_resource = PipelineResource(
        logger,
        metrics,
        "tests.metrics_contract.PipelineResource",
        resource_id="contract-pipeline-resource",
    )
    pipeline_resource.transitive_resource = transitive_resource
    transitive_resource._mn_validate_and_wrap_public_async_methods()
    pipeline_resource._mn_validate_and_wrap_public_async_methods()

    class PipelineUsingResource(Pipeline[EmptyEvent]):
        async def produce_event(self) -> EmptyEvent:
            await pipeline_resource.load_value()
            return EmptyEvent()

    pipeline = PipelineUsingResource(
        "contract-pipeline",
        "tests.metrics_contract.PipelineUsingResource",
        metrics,
        logger,
    )
    await pipeline._mn_produce_and_fan_out_event()

    pipeline_resource_value = metrics.snapshot_counter_value(
        RESOURCE_SERVES_TOTAL,
        {
            LABEL_RESOURCE: "contract-pipeline-resource",
            LABEL_RESOURCE_METHOD: "load_value",
            LABEL_RESOURCE_CALLER_KIND: "pipeline",
            LABEL_RESOURCE_CALLER: "contract-pipeline",
            LABEL_ORCHESTRATION_ID: "",
        },
    )
    assert pipeline_resource_value == 1
    transitive_resource_value = metrics.snapshot_counter_value(
        RESOURCE_SERVES_TOTAL,
        {
            LABEL_RESOURCE: "contract-transitive-resource",
            LABEL_RESOURCE_METHOD: "read_value",
            LABEL_RESOURCE_CALLER_KIND: "resource",
            LABEL_RESOURCE_CALLER: "contract-pipeline-resource",
            LABEL_ORCHESTRATION_ID: "",
        },
    )
    assert transitive_resource_value == 1


@pytest.mark.asyncio
async def test_resource_runtime_metric_labels_match_contract(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    async def succeed() -> str:
        return "ok"

    async def fail() -> str:
        raise ValueError("boom")

    resource = Resource(
        logger,
        metrics,
        "tests.metrics_contract.Resource",
        resource_id="contract-resource",
    )

    assert await resource._mn_run_with_tracking("succeed", succeed) == "ok"
    with pytest.raises(ValueError):
        await resource._mn_run_with_tracking("fail", fail)

    metrics.assert_metric_label_observations_match_contract()
    serve_value = metrics.snapshot_counter_value(
        RESOURCE_SERVES_TOTAL,
        {
            LABEL_RESOURCE: "contract-resource",
            LABEL_RESOURCE_METHOD: "succeed",
            LABEL_RESOURCE_CALLER_KIND: "unknown",
            LABEL_RESOURCE_CALLER: "",
            LABEL_ORCHESTRATION_ID: "",
        },
    )
    assert serve_value == 1


@pytest.mark.asyncio
async def test_minion_runtime_metric_labels_match_contract(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    class SuccessMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_one(self):
            pass

    class FailureMinion(Minion[EmptyEvent, EmptyContext]):
        @minion_step
        async def step_one(self):
            raise ValueError("boom")

    success_minion = SuccessMinion(
        "contract-success-minion",
        "contract-success",
        "tests.metrics_contract.SuccessMinion",
        None,
        NoOpStateStore(),
        metrics,
        logger,
        minion_id="contract-success-minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )
    success_minion._mn_mark_running()
    await success_minion._mn_accept_event(EmptyEvent())

    failure_minion = FailureMinion(
        "contract-failure-minion",
        "contract-failure",
        "tests.metrics_contract.FailureMinion",
        None,
        NoOpStateStore(),
        metrics,
        logger,
        minion_id="contract-failure-minion",
        minion_config_id="",
        pipeline_id="dummy-pipeline-id",
    )
    failure_minion._mn_mark_running()
    await failure_minion._mn_accept_event(EmptyEvent())

    metrics.assert_metric_label_observations_match_contract()
