from typing import Literal, overload

from minions._internal._framework.metrics import Kind
from minions._internal._framework.metrics_interface import (
    LabelledCounter,
    LabelledGauge,
    LabelledHistogram,
    LabelledMetric,
)
from tests.assets.crash.boom import boom
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class AssetMetrics(InMemoryMetrics):
    @overload
    def create_metric(
        self,
        metric_name: str,
        label_names: list[str],
        kind: Literal["counter"],
    ) -> LabelledCounter: ...

    @overload
    def create_metric(
        self,
        metric_name: str,
        label_names: list[str],
        kind: Literal["gauge"],
    ) -> LabelledGauge: ...

    @overload
    def create_metric(
        self,
        metric_name: str,
        label_names: list[str],
        kind: Literal["histogram"],
    ) -> LabelledHistogram: ...

    def create_metric(self, metric_name: str, label_names: list[str], kind: Kind) -> LabelledMetric:
        boom()
