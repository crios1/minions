import threading
from collections.abc import Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Generator, Literal, cast
from wsgiref.simple_server import WSGIServer

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, start_http_server

from .logger import ERROR, Logger
from .metric_definition import Kind, MetricDefinition
from .metrics import (
    HistogramSample,
    Metrics,
    SnapshotCounters,
    SnapshotGauges,
    SnapshotHistograms,
    SnapshotResult,
)

_NativeCollector = Counter | Gauge | Histogram


@dataclass(frozen=True, slots=True)
class _RegisteredMetric:
    definition: MetricDefinition[Kind]
    collector: _NativeCollector


@dataclass(frozen=True, slots=True)
class _CollectorRegistryState:
    collector_registry: CollectorRegistry
    metrics_by_name: dict[str, _RegisteredMetric]


class PrometheusMetrics(Metrics):
    """Expose Minions metrics to Prometheus."""

    def __init__(
        self,
        logger: Logger,
        port: int = 8081,
        addr: str = "",
    ):
        super().__init__(logger)
        self._port = port
        self._addr = addr
        self._lifecycle_lock = threading.Lock()
        self._http_server: tuple[WSGIServer, threading.Thread] | None = None
        self._collector_registry_state = self._new_collector_registry_state()

    def _new_collector_registry_state(self) -> _CollectorRegistryState:
        collector_registry = CollectorRegistry()
        return _CollectorRegistryState(collector_registry, {})

    async def startup(self) -> None:
        try:
            with self._lifecycle_lock:
                if self._http_server is not None:
                    return
                self._http_server = start_http_server(
                    port=self._port,
                    addr=self._addr,
                    registry=self._collector_registry_state.collector_registry,
                )
        except Exception as error:
            await self._mn_logger._mn_log_exception(
                ERROR,
                "[Prometheus] Failed to start metrics HTTP server",
                error,
            )
            raise

    async def shutdown(self) -> None:
        with self._lifecycle_lock:
            self._stop_http_server()
            self._http_server = None

    def _stop_http_server(self) -> None:
        if self._http_server is None:
            return

        server, thread = self._http_server
        try:
            server.shutdown()
        finally:
            try:
                server.server_close()
            finally:
                if thread is not threading.current_thread():
                    thread.join()

    def _create_metric(
        self,
        definition: MetricDefinition[Kind],
    ) -> _NativeCollector:
        registry = self._collector_registry_state.collector_registry
        label_names = list(definition.label_names)
        if definition.kind == "counter":
            return Counter(
                definition.name,
                f"{definition.name} ({definition.kind})",
                labelnames=label_names,
                registry=registry,
            )
        if definition.kind == "gauge":
            return Gauge(
                definition.name,
                f"{definition.name} ({definition.kind})",
                labelnames=label_names,
                registry=registry,
            )
        if definition.kind == "histogram":
            return Histogram(
                definition.name,
                f"{definition.name} ({definition.kind})",
                labelnames=label_names,
                registry=registry,
                buckets=(
                    definition.histogram_buckets
                    if definition.histogram_buckets is not None
                    else Histogram.DEFAULT_BUCKETS
                ),
            )
        raise ValueError(f"[Prometheus] Unknown metric kind: {definition.kind}")

    @contextmanager
    def _metric_for_update(
        self,
        definition: MetricDefinition[Kind],
    ) -> Generator[_NativeCollector, None, None]:
        with self._lifecycle_lock:
            registry_state = self._collector_registry_state
            registered_metric = registry_state.metrics_by_name.get(definition.name)
            if registered_metric:
                self._mn_require_same_definition_for_metric_name(
                    registered_metric.definition,
                    definition,
                )
                metric = registered_metric.collector
            else:
                metric = self._create_metric(definition)
                registry_state.metrics_by_name[definition.name] = _RegisteredMetric(
                    definition,
                    metric,
                )
            yield metric

    def increment_counter(
        self,
        definition: MetricDefinition[Literal["counter"]],
        *,
        labels: Mapping[str, str],
        amount: float = 1,
    ) -> None:
        with self._metric_for_update(definition) as metric:
            counter = cast(Counter, metric)
            if labels:
                counter.labels(**dict(labels)).inc(amount=amount)
            else:
                counter.inc(amount=amount)

    def set_gauge(
        self,
        definition: MetricDefinition[Literal["gauge"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        with self._metric_for_update(definition) as metric:
            gauge = cast(Gauge, metric)
            if labels:
                gauge.labels(**dict(labels)).set(value)
            else:
                gauge.set(value)

    def observe_histogram(
        self,
        definition: MetricDefinition[Literal["histogram"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        with self._metric_for_update(definition) as metric:
            histogram = cast(Histogram, metric)
            if labels:
                histogram.labels(**dict(labels)).observe(value)
            else:
                histogram.observe(value)

    def snapshot(self) -> SnapshotResult:
        with self._lifecycle_lock:
            registry_state = self._collector_registry_state
            counters: SnapshotCounters = {}
            gauges: SnapshotGauges = {}
            histograms: SnapshotHistograms = {}
            histogram_values: dict[
                tuple[str, tuple[tuple[str, str], ...]], HistogramSample
            ] = {}
            registered_metrics = tuple(registry_state.metrics_by_name.values())

            definition_names_by_prometheus_family = {
                (
                    metric.definition.name.removesuffix("_total")
                    if metric.definition.kind == "counter"
                    else metric.definition.name
                ): metric.definition.name
                for metric in registered_metrics
            }

            for metric_family in registry_state.collector_registry.collect():
                metric_name = definition_names_by_prometheus_family[metric_family.name]
                if metric_family.type == "counter":
                    for sample in metric_family.samples:
                        if sample.name.endswith("_created"):
                            continue
                        counters.setdefault(metric_name, []).append(
                            {
                                "labels": dict(sample.labels or {}),
                                "value": float(sample.value),
                            }
                        )
                elif metric_family.type == "gauge":
                    for sample in metric_family.samples:
                        gauges.setdefault(metric_name, []).append(
                            {
                                "labels": dict(sample.labels or {}),
                                "value": float(sample.value),
                            }
                        )
                elif metric_family.type == "histogram":
                    for sample in metric_family.samples:
                        if not sample.name.endswith(("_count", "_sum")):
                            continue
                        labels = tuple(sorted((sample.labels or {}).items()))
                        key = (metric_name, labels)
                        record = histogram_values.get(key)
                        if record is None:
                            record = cast(
                                HistogramSample,
                                {
                                    "labels": dict(sample.labels or {}),
                                    "count": 0.0,
                                    "sum": 0.0,
                                },
                            )
                            histogram_values[key] = record
                        if sample.name.endswith("_count"):
                            record["count"] = float(sample.value)
                        elif sample.name.endswith("_sum"):
                            record["sum"] = float(sample.value)

            for (metric_name, _), record in histogram_values.items():
                histograms.setdefault(metric_name, []).append(record)

            return {"counter": counters, "gauge": gauges, "histogram": histograms}
