from minions._internal._framework.metrics import Metrics

from .mixin_spy import SpyMixin


class SpiedMetrics(SpyMixin, Metrics):
    pass
