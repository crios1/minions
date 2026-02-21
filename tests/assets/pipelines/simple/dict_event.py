from tests.assets.support.pipeline_spied import SpiedPipeline


class DictPipeline(SpiedPipeline[dict]):
    async def produce_event(self):
        return {}


pipeline = DictPipeline
