import pytest
from minions import Pipeline


class TestPipelineSubclassingInvalid:
    def test_missing_event_type(self):
        with pytest.raises(TypeError) as excinfo:
            class SomePipeline(Pipeline):
                async def produce_event(self):  # pragma: no cover
                    ...
        assert str(excinfo.value) == (
            "SomePipeline must declare an event type. "
            "Example: class MyPipeline(Pipeline[MyEvent])"
        )
