import threading
from typing import Literal, overload
from wsgiref.simple_server import WSGIServer

from prometheus_client import (
    REGISTRY,
    CollectorRegistry,
    Counter,
    Gauge,
    Histogram,
    start_http_server,
)

from .logger import ERROR, Logger
from .metrics import (
    HistogramSample,
    Kind,
    Metrics,
    SnapshotCounters,
    SnapshotGauges,
    SnapshotHistograms,
)
from .metrics_constants import (
    STATE_STORE_PAYLOAD_SIZE_BYTES,
)
from .metrics_interface import LabelledCounter, LabelledGauge, LabelledHistogram, LabelledMetric

_PAYLOAD_SIZE_BYTES_HISTOGRAM_BUCKETS = (
    256.0,
    1024.0,
    4096.0,
    16_384.0,
    65_536.0,
    262_144.0,
    1_048_576.0,
    4_194_304.0,
    16_777_216.0,
    67_108_864.0,
)
_HISTOGRAM_BUCKETS_BY_METRIC: dict[str, tuple[float, ...]] = {
    STATE_STORE_PAYLOAD_SIZE_BYTES: _PAYLOAD_SIZE_BYTES_HISTOGRAM_BUCKETS,
}


class _PrometheusCounter:
    def __init__(self, metric: Counter, label_names: list[str]):
        self._metric = metric
        self._label_names = tuple(label_names)

    def labels(self, **kwargs: str) -> LabelledCounter:
        if not self._label_names and not kwargs:
            return self
        return _PrometheusCounter(self._metric.labels(**kwargs), list(self._label_names))

    def inc(self, amount: float = 1):
        return self._metric.inc(amount=amount)


class _PrometheusGauge:
    def __init__(self, metric: Gauge, label_names: list[str]):
        self._metric = metric
        self._label_names = tuple(label_names)

    def labels(self, **kwargs: str) -> LabelledGauge:
        if not self._label_names and not kwargs:
            return self
        return _PrometheusGauge(self._metric.labels(**kwargs), list(self._label_names))

    def set(self, value: float):
        return self._metric.set(value)


class _PrometheusHistogram:
    def __init__(self, metric: Histogram, label_names: list[str]):
        self._metric = metric
        self._label_names = tuple(label_names)

    def labels(self, **kwargs: str) -> LabelledHistogram:
        if not self._label_names and not kwargs:
            return self
        return _PrometheusHistogram(self._metric.labels(**kwargs), list(self._label_names))

    def observe(self, value: float):
        return self._metric.observe(value)


class PrometheusMetrics(Metrics):
    """
    Prometheus-backed metrics registry. Exposes a /metrics endpoint on the specified port.
    Implements create_metric(...) by adapting native prometheus_client metric instances.
    """

    def __init__(
        self,
        logger: Logger,
        port: int = 8081,
        addr: str = "",
        registry: CollectorRegistry = REGISTRY,
    ):
        super().__init__(logger)
        self._port = port
        self._addr = addr
        self._registry = registry
        self._started = False
        self._started_lock = threading.Lock()
        self._http_server: tuple[WSGIServer, threading.Thread] | None = None

    async def startup(self) -> None:
        try:
            with self._started_lock:
                if not self._started:
                    self._http_server = start_http_server(
                        port=self._port,
                        addr=self._addr,
                        registry=self._registry,
                    )
                    self._started = True
        except Exception as e:
            await self._mn_logger._mn_log_exception(
                ERROR,
                "[Prometheus] Failed to start metrics HTTP server",
                e,
            )

    async def shutdown(self) -> None:
        with self._started_lock:
            http_server = self._http_server
            self._http_server = None
            self._started = False

        if http_server is None:
            return

        server, thread = http_server
        try:
            server.shutdown()
        finally:
            server.server_close()
            if thread is not threading.current_thread():
                thread.join()

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

    def create_metric(
        self, metric_name: str, label_names: list[str], kind: Kind
    ) -> LabelledMetric:
        """Create and return a Prometheus metric with the given name and labels."""

        if kind == "counter":
            return _PrometheusCounter(
                Counter(
                    metric_name,
                    f"{metric_name} ({kind})",
                    labelnames=label_names,
                    registry=self._registry,
                ),
                label_names,
            )
        if kind == "gauge":
            return _PrometheusGauge(
                Gauge(
                    metric_name,
                    f"{metric_name} ({kind})",
                    labelnames=label_names,
                    registry=self._registry,
                ),
                label_names,
            )
        if kind == "histogram":
            buckets = _HISTOGRAM_BUCKETS_BY_METRIC.get(
                metric_name,
                Histogram.DEFAULT_BUCKETS,
            )
            histogram = Histogram(
                metric_name,
                f"{metric_name} ({kind})",
                labelnames=label_names,
                registry=self._registry,
                buckets=buckets,
            )
            return _PrometheusHistogram(
                histogram,
                label_names,
            )

        raise ValueError(f"[Prometheus] Unknown metric kind: {kind}")

    def snapshot_counters(self) -> SnapshotCounters:
        out: SnapshotCounters = {}
        for mf in self._registry.collect():
            if mf.type != "counter":
                continue
            for s in mf.samples:
                if s.name.endswith("_created"):
                    continue
                name = s.name[:-6] if s.name.endswith("_total") else s.name
                out.setdefault(name, []).append(
                    {"labels": dict(s.labels or {}), "value": float(s.value)}
                )
        return out

    def snapshot_gauges(self) -> SnapshotGauges:
        out: SnapshotGauges = {}
        for mf in self._registry.collect():
            if mf.type != "gauge":
                continue
            for s in mf.samples:
                out.setdefault(s.name, []).append(
                    {"labels": dict(s.labels or {}), "value": float(s.value)}
                )
        return out

    def snapshot_histograms(self) -> SnapshotHistograms:
        out: SnapshotHistograms = {}
        tmp: dict[tuple[str, tuple[tuple[str, str], ...]], HistogramSample] = {}
        # group by (metric_name, label_set) becasue mf.samples intermix *_count and *_sum

        for mf in self._registry.collect():
            if mf.type != "histogram":
                continue
            for s in mf.samples:
                lbls = tuple(sorted((s.labels or {}).items()))
                key = (mf.name, lbls)
                rec = tmp.get(key)
                if rec is None:
                    new_rec: HistogramSample = {
                        "labels": dict(s.labels or {}),
                        "count": 0.0,
                        "sum": 0.0,
                    }
                    tmp[key] = new_rec
                    rec = new_rec
                n = s.name
                v = float(s.value)
                if n.endswith("_count"):
                    rec["count"] = v
                elif n.endswith("_sum"):
                    rec["sum"] = v

        # flatten grouped series into per metric lists
        for (metric_name, _), rec in tmp.items():
            out.setdefault(metric_name, []).append(rec)

        return out
