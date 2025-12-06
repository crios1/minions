from .metrics import Metrics, SnapshotCounters, SnapshotGauges, SnapshotHistograms
from .metrics_interface import LabelledMetric
from .logger_noop import NoOpLogger

class _NoOpMetric:
    def labels(self, **kwargs: str) -> "LabelledMetric":
        return self
    def inc(self, amount: float = 1): pass
    def set(self, value: float): pass
    def observe(self, value: float): pass

class NoOpMetrics(Metrics):
    """
    No-op metrics backend for testing or disabled environments.
    Does nothing but satisfies the framework’s expectations.
    """

    def __init__(self):
        super().__init__(NoOpLogger()) 
        self._noop_metric = _NoOpMetric()

    def create_metric(self, metric_name: str, label_names: list[str], kind: str) -> LabelledMetric:
        return self._noop_metric

    def snapshot_counters(self) -> SnapshotCounters:
        return {}

    def snapshot_gauges(self) -> SnapshotGauges:
        return {}
    
    def snapshot_histograms(self) -> SnapshotHistograms:
        return {}