from minions._internal._domain.pipeline import Pipeline
from .mixin_spy import SpyMixin

from minions._internal._domain.types import T_Event

class SpiedPipeline(SpyMixin, Pipeline[T_Event], defer_pipeline_setup=True):
    _mn_user_facing = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
