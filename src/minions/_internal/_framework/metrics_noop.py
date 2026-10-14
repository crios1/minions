from collections.abc import Mapping
from typing import Literal

from .logger_noop import NoOpLogger
from .metric_definition import MetricDefinition
from .metrics import Metrics, SnapshotResult


class NoOpMetrics(Metrics):
    """Metrics backend that discards operations and retains no runtime state."""

    def __init__(self):
        super().__init__(NoOpLogger())

    def increment_counter(
        self,
        definition: MetricDefinition[Literal["counter"]],
        *,
        labels: Mapping[str, str],
        amount: float = 1,
    ) -> None:
        pass

    def set_gauge(
        self,
        definition: MetricDefinition[Literal["gauge"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        pass

    def observe_histogram(
        self,
        definition: MetricDefinition[Literal["histogram"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        pass

    def snapshot(self) -> SnapshotResult:
        return {"counter": {}, "gauge": {}, "histogram": {}}
