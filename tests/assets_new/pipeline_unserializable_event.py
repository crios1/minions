from tests.assets.support.pipeline_spied import SpiedPipeline


class UnserializableEvent:
    pass


class UnserializableEventPipeline(SpiedPipeline[UnserializableEvent]):
    async def produce_event(self) -> UnserializableEvent:
        raise RuntimeError("should not run")


pipeline = UnserializableEventPipeline
