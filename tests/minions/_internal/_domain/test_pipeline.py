import pytest
import msgspec
from minions import Pipeline
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import (
    LABEL_ERROR_TYPE,
    LABEL_PIPELINE,
    PIPELINE_ERROR_TOTAL,
)
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class MyStructEvent(msgspec.Struct):
    ts: int


class TestPipelineSubclassingValid:
    def test_valid_msgspec_struct_event_type(self):
        class SomePipeline(Pipeline[MyStructEvent]):
            async def produce_event(self):
                return MyStructEvent(1)


class TestPipelineSubclassingInvalid:
    def test_missing_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline):
                async def produce_event(self):  # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline must declare an event type "
            "(e.g. class MyPipeline(Pipeline[MyEvent]): ...)."
        )


@pytest.mark.asyncio
async def test_pipeline_error_metric_includes_error_type():
    class ErrorPipeline(Pipeline[MyStructEvent]):
        async def produce_event(self):
            raise RuntimeError("boom")

    metrics = InMemoryMetrics()
    pipeline = ErrorPipeline(
        "test_pipeline",
        "tests.minions._internal._domain.test_pipeline.ErrorPipeline",
        metrics,
        NoOpLogger(),
    )

    with pytest.raises(RuntimeError):
        await pipeline._mn_produce_and_handle_event()

    counters = metrics.snapshot_counters()
    sample = InMemoryMetrics.find_sample(
        counters[PIPELINE_ERROR_TOTAL],
        {
            LABEL_PIPELINE: "test_pipeline",
            LABEL_ERROR_TYPE: "RuntimeError",
        },
    )
    assert sample["value"] == 1.0
