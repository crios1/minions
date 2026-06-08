"""Framework Metrics.

To create a custom metrics backend:
1. Subclass `Metrics`
2. Implement `create_metric(...)`, returning an object that conforms to the
   metric kind

The framework will automatically:
- Look up metric label names from METRIC_LABEL_NAMES
- Handle registry, locking, and lifecycle
- Call `.labels(**labels).inc()/set()/observe()` on the returned objects
"""

import asyncio
import threading
from abc import abstractmethod
from typing import Literal, TypedDict, overload

from minions._internal._utils.safe_create_task import safe_create_task

from .async_component import AsyncComponent
from .logger import WARNING, Logger
from .metrics_constants import METRIC_LABEL_NAMES
from .metrics_interface import (
    LabelledCounter,
    LabelledGauge,
    LabelledHistogram,
    LabelledMetric,
)

Kind = Literal["counter", "gauge", "histogram"]
Labels = dict[str, str]


class CounterSample(TypedDict):
    labels: dict[str, str]
    value: float


class GaugeSample(TypedDict):
    labels: dict[str, str]
    value: float


class HistogramSample(TypedDict):
    labels: dict[str, str]
    count: float
    sum: float


SnapshotCounters = dict[str, list[CounterSample]]
SnapshotGauges = dict[str, list[GaugeSample]]
SnapshotHistograms = dict[str, list[HistogramSample]]

SnapshotResult = dict[Kind, SnapshotCounters | SnapshotGauges | SnapshotHistograms]


class Metrics(AsyncComponent):
    """
    To implement your own metrics backend, subclass this and override:

    - create_metric(metric_name, label_names, kind)
    - snapshot_counters()
    - snapshot_gauges()
    - snapshot_histograms()

    Label names are managed by the framework; you only handle metric creation.
    """

    _mn_user_facing = True

    def __init__(self, logger: Logger):
        super().__init__(logger)
        self._mn_registries: dict[Kind, dict[str, LabelledMetric]] = {
            "counter": {},
            "gauge": {},
            "histogram": {},
        }
        self._mn_locks: dict[Kind, threading.Lock] = {
            "counter": threading.Lock(),
            "gauge": threading.Lock(),
            "histogram": threading.Lock(),
        }
        self._mn_unknown_metrics: set[str] = set()

    def _mn_get_label_names_for_metric(self, metric_name: str) -> list[str]:
        labels = METRIC_LABEL_NAMES.get(metric_name, [])
        if not labels and metric_name not in self._mn_unknown_metrics:
            self._mn_unknown_metrics.add(metric_name)
            safe_create_task(
                self._mn_logger._mn_log(
                    WARNING,
                    f"metrics: unknown metric '{metric_name}', using no labels",
                )
            )
        return labels

    @overload
    def _mn_get_metric_unsafe(
        self, kind: Literal["counter"], metric_name: str
    ) -> LabelledCounter: ...

    @overload
    def _mn_get_metric_unsafe(
        self, kind: Literal["gauge"], metric_name: str
    ) -> LabelledGauge: ...

    @overload
    def _mn_get_metric_unsafe(
        self, kind: Literal["histogram"], metric_name: str
    ) -> LabelledHistogram: ...

    def _mn_get_metric_unsafe(self, kind: Kind, metric_name: str) -> LabelledMetric:
        registry = self._mn_registries[kind]
        if metric := registry.get(metric_name):
            return metric
        with self._mn_locks[kind]:
            if metric := registry.get(metric_name):
                return metric
            labels = self._mn_get_label_names_for_metric(metric_name)
            metric = self.create_metric(metric_name, labels, kind)
            registry[metric_name] = metric
            return metric

    def _mn_inc_unsafe(self, metric_name: str, amount: float = 1, labels: Labels | None = None):
        metric = self._mn_get_metric_unsafe("counter", metric_name)
        metric.labels(**(labels or {})).inc(amount=amount)

    def _mn_set_unsafe(self, metric_name: str, value: float, labels: Labels | None = None):
        metric = self._mn_get_metric_unsafe("gauge", metric_name)
        metric.labels(**(labels or {})).set(value)

    def _mn_observe_unsafe(self, metric_name: str, value: float, labels: Labels | None = None):
        metric = self._mn_get_metric_unsafe("histogram", metric_name)
        metric.labels(**(labels or {})).observe(value)

    async def _mn_inc(self, metric_name: str, amount: float = 1, labels: Labels | None = None):
        """Increment a counter by the given amount (positive or negative)."""
        return await self._mn_safe_run_and_log_failure(
            method=self._mn_inc_unsafe,
            method_args=[metric_name, amount, labels],
        )

    async def _mn_set(self, metric_name: str, value: float, labels: Labels | None = None):
        """Set a gauge to a specific value."""
        return await self._mn_safe_run_and_log_failure(
            method=self._mn_set_unsafe,
            method_args=[metric_name, value, labels],
        )

    async def _mn_observe(self, metric_name: str, value: float, labels: Labels | None = None):
        """Observe a value (for histograms or summaries)."""
        return await self._mn_safe_run_and_log_failure(
            method=self._mn_observe_unsafe,
            method_args=[metric_name, value, labels],
        )

    async def _mn_snapshot(self) -> SnapshotResult:
        counters, gagues, histograms = await asyncio.gather(
            self._mn_safe_run_and_log_failure(self.snapshot_counters),
            self._mn_safe_run_and_log_failure(self.snapshot_gauges),
            self._mn_safe_run_and_log_failure(self.snapshot_histograms),
        )
        return {
            "counter": counters if counters else {},
            "gauge": gagues if gagues else {},
            "histogram": histograms if histograms else {},
        }

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

    @abstractmethod
    def create_metric(self, metric_name: str, label_names: list[str], kind: Kind) -> LabelledMetric:
        """Create and return a backend-specific metric object.

        The framework will call `.labels(...).inc()/set()/observe()` on it.
        """

    @abstractmethod
    def snapshot_counters(self) -> SnapshotCounters:
        """Return a snapshot of all counters.

        Example structure::

            {
                "MINION_WORKFLOW_SUCCEEDED_TOTAL": [
                    {"labels": {"minion": "PriceSync", "reason": "start"}, "value": 4.0},
                    {"labels": {"minion": "PriceSync", "reason": "resume"}, "value": 2.0},
                    {"labels": {"minion": "OrderWatcher", "reason": "start"}, "value": 1.0}
                ],
                "MINION_WORKFLOW_FAILED_TOTAL": [
                    {
                        "labels": {"minion": "OrderWatcher", "error_type": "TimeoutError"},
                        "value": 1.0,
                    }
                ],
                "MINION_WORKFLOW_ABORTED_TOTAL": []
            }
        """

    @abstractmethod
    def snapshot_gauges(self) -> SnapshotGauges:
        """Return a snapshot of all gauges.

        Example structure::

            {
                "MINION_WORKFLOW_INFLIGHT_GAUGE": [
                    {"labels": {"minion": "PriceSync"}, "value": 1.0},
                    {"labels": {"minion": "OrderWatcher"}, "value": 0.0}
                ],
                "SYSTEM_MEMORY_USAGE_BYTES": [
                    {"labels": {}, "value": 2.14e8}
                ]
            }
        """

    @abstractmethod
    def snapshot_histograms(self) -> SnapshotHistograms:
        """Return a snapshot of all histograms.

        Example structure::

            {
                "STEP_DURATION_SECONDS": [
                    {
                        "labels": {"minion": "PriceSync", "step": "fetch_orders"},
                        "count": 3.0,
                        "sum": 1.24,
                    },
                    {
                        "labels": {"minion": "PriceSync", "step": "sync_prices"},
                        "count": 2.0,
                        "sum": 0.77,
                    },
                    {
                        "labels": {"minion": "OrderWatcher", "step": "poll_market"},
                        "count": 5.0,
                        "sum": 2.95,
                    }
                ],
                "WORKFLOW_LATENCY_SECONDS": [
                    {"labels": {"minion": "PriceSync"}, "count": 10.0, "sum": 4.38}
                ]
            }
        """
