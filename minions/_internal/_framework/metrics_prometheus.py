import traceback
import threading
from prometheus_client import (
    CollectorRegistry, REGISTRY,
    Counter, Gauge, Histogram, start_http_server
)

from .metrics import (
    Metrics, Kind,
    SnapshotCounters, SnapshotGauges, SnapshotHistograms,
    CounterSample, GaugeSample, HistogramSample
)
from .logger import Logger, ERROR
from .metrics_interface import LabelledMetric

class PrometheusMetrics(Metrics):
    """
    Prometheus-backed metrics registry. Exposes a /metrics endpoint on the specified port.
    Implements create_metric(...) by returning native prometheus_client metric instances.
    """

    def __init__(
        self,
        logger: Logger,
        port: int = 8081,
        addr: str = "",
        registry: CollectorRegistry = REGISTRY
    ):
        super().__init__(logger)
        self._port = port
        self._addr = addr
        self._registry = registry
        self._started = False
        self._started_lock = threading.Lock()

    async def startup(self):
        try:
            with self._started_lock:
                if not self._started:
                    start_http_server(port=self._port, addr=self._addr, registry=self._registry)
                    self._started = True
        except Exception as e:
            await self._mn_logger._log(
                ERROR,
                "[Prometheus] Failed to start metrics HTTP server",
                error_type=type(e).__name__,
                error_message=str(e),
                traceback="".join(traceback.format_exception(type(e), e, e.__traceback__)),
            )

    def create_metric(self, metric_name: str, label_names: list[str], kind: Kind) -> LabelledMetric:
        """
        Create and return a Prometheus metric (Counter, Gauge, or Histogram) with the given name and labels.
        """

        metric_cls = {
            "counter": Counter,
            "gauge": Gauge,
            "histogram": Histogram,
        }.get(kind)

        if metric_cls is None:
            raise ValueError(f"[Prometheus] Unknown metric kind: {kind}")

        return metric_cls(
            metric_name,
            f"{metric_name} ({kind})",
            labelnames=label_names,
            registry=self._registry
        )

    def snapshot_counters(self) -> SnapshotCounters:
        out: SnapshotCounters = {}
        for mf in self._registry.collect():
            if mf.type != 'counter':
                continue
            for s in mf.samples:
                if s.name.endswith('_created'):
                    continue
                name = s.name[:-6] if s.name.endswith('_total') else s.name
                out.setdefault(name, []).append(
                    {"labels": dict(s.labels or {}), "value": float(s.value)}
                )
        return out

    def snapshot_gauges(self) -> SnapshotGauges:
        out: SnapshotGauges = {}
        for mf in self._registry.collect():
            if mf.type != 'gauge':
                continue
            for s in mf.samples:
                out.setdefault(s.name, []).append(
                    {'labels': dict(s.labels or {}), 'value': float(s.value)}
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
                    new_rec: HistogramSample = {"labels": dict(s.labels or {}), "count": 0.0, "sum": 0.0}
                    tmp[key] = new_rec
                    rec = new_rec
                n = s.name
                v = float(s.value)
                if n.endswith('_count'):
                    rec['count'] = v
                elif n.endswith('_sum'):
                    rec['sum'] = v
        
        # flatten grouped series into per metric lists
        for (metric_name, _), rec in tmp.items():
            out.setdefault(metric_name, []).append(rec)

        return out

"""
### tests (per minion asserts) ###
snap = await metrics._snapshot()
succ = next((s for s in snap["counters"]["MINION_WORKFLOW_SUCCEEDED_TOTAL"]
             if s["labels"].get("minion") == "PriceMinion"), {"value": 0.0})
assert succ["value"] == 1.0
"""