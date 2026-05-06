from minions._internal._framework.metrics import Kind
from minions._internal._framework.metrics_interface import LabelledMetric

from tests.assets.crash.boom import boom
from tests.assets.crash.support.boom_metrics import BoomMetrics


class BoomCreateMetricMetrics(BoomMetrics):
    def create_metric(self, metric_name: str, label_names: list[str], kind: Kind) -> LabelledMetric: # pyright: ignore[reportReturnType]
        boom()
