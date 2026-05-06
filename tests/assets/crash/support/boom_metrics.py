from minions._internal._framework.metrics import (
    Kind,
    SnapshotCounters,
    SnapshotGauges,
    SnapshotHistograms,
)
from minions._internal._framework.metrics_interface import LabelledMetric
from minions._internal._framework.logger import Logger

from tests.assets.crash.boom import boom
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class _BoomMetricChild(LabelledMetric):
    def labels(self, **kwargs: str) -> "_BoomMetricChild": # pyright: ignore[reportReturnType]
        boom()

    def inc(self, amount: float = 1) -> None:
        boom()

    def set(self, value: float) -> None:
        boom()

    def observe(self, value: float) -> None:
        boom()


class BoomMetrics(InMemoryMetrics):
    def __init__(self, logger: Logger | None = None):
        super().__init__(logger)

    def create_metric(self, metric_name: str, label_names: list[str], kind: Kind) -> LabelledMetric:
        return _BoomMetricChild()

    def snapshot_counters(self) -> SnapshotCounters: # pyright: ignore[reportReturnType]
        boom()

    def snapshot_gauges(self) -> SnapshotGauges: # pyright: ignore[reportReturnType]
        boom()

    def snapshot_histograms(self) -> SnapshotHistograms: # pyright: ignore[reportReturnType]
        boom()
