import asyncio
from dataclasses import dataclass

import pytest

from minions import Minion, Pipeline, Resource, minion_step
from minions._internal._framework.logger_noop import NoOpLogger
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
    class SuccessPipeline(Pipeline[ContractEvent]):
        async def produce_event(self):
            return ContractEvent()

    class ErrorPipeline(Pipeline[ContractEvent]):
        async def produce_event(self):
            raise RuntimeError("boom")

    class FakeMinion:
        _mn_name = "ContractMinion"
        _mn_minion_instance_id = "contract-minion"
        _mn_minion_composite_key = "contract-minion-key"
        _mn_minion_modpath = "tests.metrics_contract.FakeMinion"

        def __init__(self):
            self.tasks: list[asyncio.Task] = []

        async def _mn_handle_event(self, event):
            return None

        def safe_create_task(self, coro):
            task = asyncio.create_task(coro)
            self.tasks.append(task)
            return task

    metrics = InMemoryMetrics()
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


@pytest.mark.asyncio
async def test_resource_runtime_metric_labels_match_contract():
    async def succeed():
        return "ok"

    async def fail():
        raise ValueError("boom")

    metrics = InMemoryMetrics()
    resource = Resource(NoOpLogger(), metrics, "tests.metrics_contract.Resource")

    assert await resource._mn_run_with_tracking("ContractResource", "succeed", succeed) == "ok"
    with pytest.raises(ValueError):
        await resource._mn_run_with_tracking("ContractResource", "fail", fail)

    metrics.assert_recorded_labels_match_contract()


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
