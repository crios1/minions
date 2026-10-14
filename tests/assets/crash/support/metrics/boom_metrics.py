from collections.abc import Mapping
from typing import Literal

from minions._internal._framework.metric_definition import MetricDefinition
from minions._internal._framework.metrics import SnapshotResult
from tests.assets.crash.boom import boom
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class AssetMetrics(InMemoryMetrics):
    def increment_counter(
        self,
        definition: MetricDefinition[Literal["counter"]],
        *,
        labels: Mapping[str, str],
        amount: float = 1,
    ) -> None:
        boom()

    def set_gauge(
        self,
        definition: MetricDefinition[Literal["gauge"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        boom()

    def observe_histogram(
        self,
        definition: MetricDefinition[Literal["histogram"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        boom()

    def snapshot(self) -> SnapshotResult:
        boom()
