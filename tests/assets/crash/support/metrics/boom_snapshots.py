from minions._internal._framework.metrics import (
    SnapshotCounters,
    SnapshotGauges,
    SnapshotHistograms,
)
from tests.assets.crash.boom import boom
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class AssetMetrics(InMemoryMetrics):
    def snapshot_counters(self) -> SnapshotCounters: # pyright: ignore[reportReturnType]
        boom()

    def snapshot_gauges(self) -> SnapshotGauges: # pyright: ignore[reportReturnType]
        boom()

    def snapshot_histograms(self) -> SnapshotHistograms: # pyright: ignore[reportReturnType]
        boom()
