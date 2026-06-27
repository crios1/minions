from tests.assets.support.pipeline_spied import SpiedPipeline


class UnsupportedEventType:
    pass


class AssetPipeline(SpiedPipeline[UnsupportedEventType]):
    async def produce_event(self):
        raise RuntimeError("should not run")


pipeline = AssetPipeline
