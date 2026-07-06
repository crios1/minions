# pyright: reportUnusedClass=false

from typing import Any, TypedDict

import msgspec
import pytest

from minions import Pipeline
from minions._internal._framework.metrics_constants import (
    LABEL_ERROR_TYPE,
    LABEL_PIPELINE,
    PIPELINE_ERROR_TOTAL,
)
from minions._internal._utils.serialization import SERIALIZABLE_PRIMITIVE_TYPES
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class MyStructEvent(msgspec.Struct):
    ts: int


class MyTypedDictEvent(TypedDict):
    ts: int


class TestPipelineSubclassingValid:
    def test_valid_msgspec_struct_event_type(self):
        class SomePipeline(Pipeline[MyStructEvent]):
            async def produce_event(self):
                return MyStructEvent(1)


class TestPipelineSubclassingInvalid:
    def test_missing_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline):  # pyright: ignore[reportMissingTypeArgument]
                async def produce_event(self) -> MyTypedDictEvent:  # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline must declare an event type "
            "(e.g. class MyPipeline(Pipeline[MyEvent]): ...)."
        )

    @pytest.mark.parametrize("event_type", SERIALIZABLE_PRIMITIVE_TYPES)
    def test_reject_primitive_event_type(self, event_type: type[object]):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline[event_type]):
                async def produce_event(self):  # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline: event type is not supported. "
            "Supported user-declared types: (dataclass, msgspec.Struct)."
        )

    def test_reject_any_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline[Any]):
                async def produce_event(self):  # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline: event type is not supported. "
            "Supported user-declared types: (dataclass, msgspec.Struct)."
        )

    def test_reject_bare_dict_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline[dict]):  # pyright: ignore[reportMissingTypeArgument]
                async def produce_event(self):  # pyright: ignore[reportIncompatibleMethodOverride] # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline: event type is not supported. "
            "Supported user-declared types: (dataclass, msgspec.Struct)."
        )

    def test_reject_parameterized_dict_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline[dict[str, int]]):
                async def produce_event(self):  # pyright: ignore[reportIncompatibleMethodOverride] # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline: event type is not supported. "
            "Supported user-declared types: (dataclass, msgspec.Struct)."
        )

    def test_reject_typed_dict_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline[MyTypedDictEvent]):
                async def produce_event(self) -> MyTypedDictEvent:  # pragma: no cover
                    return {"ts": 1}
        assert str(excinfo.value) == (
            "SomePipeline: event type is not supported. "
            "Supported user-declared types: (dataclass, msgspec.Struct)."
        )


@pytest.mark.asyncio
async def test_pipeline_error_metric_includes_error_type(
    logger: InMemoryLogger,
    metrics: InMemoryMetrics,
):
    class ErrorPipeline(Pipeline[MyStructEvent]):
        async def produce_event(self):
            raise RuntimeError("boom")

    pipeline = ErrorPipeline(
        "test_pipeline",
        "tests.minions._internal._domain.test_pipeline.ErrorPipeline",
        metrics,
        logger,
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
