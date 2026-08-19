from minions._internal._framework.metrics import Metrics

from .component_spy_meta import ComponentSpyMeta


class SpiedMetrics(Metrics, metaclass=ComponentSpyMeta):
    pass
