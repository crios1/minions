import asyncio
from collections.abc import Coroutine
from dataclasses import dataclass
from typing import Any

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
from minions._internal._framework.state_store_noop import NoOpStateStore
from tests.assets.support.metrics_inmemory import InMemoryMetrics


@dataclass
class ContractEvent:
    value: int = 1


@dataclass
class ContractContext:
    value: int = 0


@pytest.mark.asyncio
async def test_pipeline_runtime_metric_labels_match_contract():
    class EventValueResource(Resource):
        async def read_value(self) -> int:
            return 1

    class PipelineEventResource(Resource):
        value_source: EventValueResource

        async def build_event(self) -> ContractEvent:
            return ContractEvent(value=await self.value_source.read_value())

    metrics = InMemoryMetrics()
    value_resource = EventValueResource(
        NoOpLogger(),
        metrics,
        "tests.metrics_contract.EventValueResource",
        resource_id="contract-event-value-resource",
    )
    event_resource = PipelineEventResource(
        NoOpLogger(),
        metrics,
        "tests.metrics_contract.PipelineEventResource",
        resource_id="contract-pipeline-event-resource",
    )
    event_resource.value_source = value_resource
    value_resource._mn_validate_and_wrap_public_async_methods()
    event_resource._mn_validate_and_wrap_public_async_methods()

    class SuccessPipeline(Pipeline[ContractEvent]):
        async def produce_event(self) -> ContractEvent:
            return await event_resource.build_event()

    class ErrorPipeline(Pipeline[ContractEvent]):
        async def produce_event(self) -> ContractEvent:
            raise RuntimeError("boom")

    class FakeMinion:
        _mn_name = "ContractMinion"
        _mn_minion_instance_id = "contract-minion"
        _mn_orchestration_id = "contract-minion-key"
        _mn_minion_modpath = "tests.metrics_contract.FakeMinion"

        def __init__(self) -> None:
            self.tasks: list[asyncio.Task[None]] = []

        async def _mn_handle_event(self, event: ContractEvent) -> None:
            return None

        def safe_create_task(self, coro: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
            task = asyncio.create_task(coro)
            self.tasks.append(task)
            return task

    success_pipeline = SuccessPipeline(
        "contract-pipeline",
        "tests.metrics_contract.SuccessPipeline",
        metrics,
        NoOpLogger(),
    )
    fake_minion = FakeMinion()
    success_pipeline._mn_subs.add(fake_minion)  # type: ignore[arg-type]

    await success_pipeline._mn_produce_and_handle_event()
    await asyncio.gather(*fake_minion.tasks)

    error_pipeline = ErrorPipeline(
        "contract-error-pipeline",
        "tests.metrics_contract.ErrorPipeline",
        metrics,
        NoOpLogger(),
    )
    with pytest.raises(RuntimeError):
        await error_pipeline._mn_produce_and_handle_event()

    metrics.assert_recorded_labels_match_contract()
    # Resource metrics preserve the immediate caller: pipeline -> resource -> nested resource.
    serve_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_counters()[RESOURCE_SERVES_TOTAL],
        {
            LABEL_RESOURCE: "contract-pipeline-event-resource",
            LABEL_RESOURCE_METHOD: "build_event",
            LABEL_RESOURCE_CALLER_KIND: "pipeline",
            LABEL_RESOURCE_CALLER: "contract-pipeline",
            LABEL_ORCHESTRATION_ID: "",
        },
    )
    assert serve_sample["value"] == 1
    nested_serve_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_counters()[RESOURCE_SERVES_TOTAL],
        {
            LABEL_RESOURCE: "contract-event-value-resource",
            LABEL_RESOURCE_METHOD: "read_value",
            LABEL_RESOURCE_CALLER_KIND: "resource",
            LABEL_RESOURCE_CALLER: "contract-pipeline-event-resource",
            LABEL_ORCHESTRATION_ID: "",
        },
    )
    assert nested_serve_sample["value"] == 1


@pytest.mark.asyncio
async def test_resource_runtime_metric_labels_match_contract():
    async def succeed() -> str:
        return "ok"

    async def fail() -> str:
        raise ValueError("boom")

    metrics = InMemoryMetrics()
    resource = Resource(NoOpLogger(), metrics, "tests.metrics_contract.Resource")

    assert await resource._mn_run_with_tracking("ContractResource", "succeed", succeed) == "ok"
    with pytest.raises(ValueError):
        await resource._mn_run_with_tracking("ContractResource", "fail", fail)

    metrics.assert_recorded_labels_match_contract()
    serve_sample = InMemoryMetrics.find_sample(
        metrics.snapshot_counters()[RESOURCE_SERVES_TOTAL],
        {
            LABEL_RESOURCE: "ContractResource",
            LABEL_RESOURCE_METHOD: "succeed",
            LABEL_RESOURCE_CALLER_KIND: "unknown",
            LABEL_RESOURCE_CALLER: "",
            LABEL_ORCHESTRATION_ID: "",
        },
    )
    assert serve_sample["value"] == 1


@pytest.mark.asyncio
async def test_minion_runtime_metric_labels_match_contract():
    class SuccessMinion(Minion[ContractEvent, ContractContext]):
        @minion_step
        async def step_one(self):
            self.context.value += self.event.value

    class FailureMinion(Minion[ContractEvent, ContractContext]):
        @minion_step
        async def step_one(self):
            raise ValueError("boom")

    metrics = InMemoryMetrics()
    success_minion = SuccessMinion(
        "contract-success-minion",
        "contract-success",
        "tests.metrics_contract.SuccessMinion",
        None,
        NoOpStateStore(),
        metrics,
        NoOpLogger(),
    )
    success_minion._mn_started.set()
    await success_minion._mn_handle_event(ContractEvent())

    failure_minion = FailureMinion(
        "contract-failure-minion",
        "contract-failure",
        "tests.metrics_contract.FailureMinion",
        None,
        NoOpStateStore(),
        metrics,
        NoOpLogger(),
    )
    failure_minion._mn_started.set()
    await failure_minion._mn_handle_event(ContractEvent())

    metrics.assert_recorded_labels_match_contract()
