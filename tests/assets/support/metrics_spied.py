from minions._internal._framework.metrics import Metrics

from .mixin_spy import SpyMixin


class SpiedMetrics(SpyMixin, Metrics):
    _mn_user_facing = True

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
