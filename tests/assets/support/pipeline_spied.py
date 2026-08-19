from minions._internal._domain.pipeline import Pipeline
from minions._internal._domain.types import T_Event

from .component_spy_meta import ComponentSpyMeta


class SpiedPipeline(
    Pipeline[T_Event],
    defer_pipeline_setup=True,
    metaclass=ComponentSpyMeta,
):
    pass
