from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.types import T_Event

from .mixin_spy import SpyMixin


class SpiedPipeline(SpyMixin, Pipeline[T_Event], defer_pipeline_setup=True):
    pass
