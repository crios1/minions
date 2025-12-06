from minions import Pipeline

from .support.mixin_spy import SpyMixin

class DictPipeline(SpyMixin, Pipeline[dict]):
    async def produce_event(self):
        return {}

# pipeline = DictPipeline