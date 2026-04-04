import pytest
import msgspec
from minions import Pipeline


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
