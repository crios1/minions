from tests.assets.support.pipeline_spied import SpiedPipeline


class DictEventPipeline(SpiedPipeline[dict[str, int]]):
    async def produce_event(self) -> dict[str, int]:
        return {"seq": 1}


pipeline = DictEventPipeline
