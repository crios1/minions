import threading
from typing import Any, Dict, Tuple

from minions._internal._framework.metrics import Metrics
from minions._internal._framework.metrics_interface import LabelledMetric
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import METRIC_LABEL_NAMES

from .mixin_spy import SpyMixin

LabelKey = Tuple[Tuple[str, str], ...]  # sorted (name, value) pairs for hashing


def _normalize_labels(metric_name: str, labels: Dict[str, Any]) -> LabelKey:
    """
    Normalize labels to a stable, hashable key:
    - If metric is known in METRIC_LABEL_NAMES, use that order and default missing to "".
    - Otherwise, accept whatever was provided; sort by key.
    - Coerce values to str for stability.
    """
    expected = METRIC_LABEL_NAMES.get(metric_name, [])
    if expected:
        items = tuple((name, str(labels.get(name, ""))) for name in expected)
    else:
        items = tuple(sorted((k, str(v)) for k, v in labels.items()))
    return items


class _InMemoryMetricChild(LabelledMetric):
    """
    A LabelledMetric bound to a specific label set (label_key).
    Implements the LabelledMetric protocol: labels/inc/set/observe.
    """
    def __init__(self, parent: "_InMemoryMetric", label_key: LabelKey):
        self._parent = parent
        self._label_key = label_key

    # Protocol: def labels(self, **kwargs: str) -> "LabelledMetric"
    def labels(self, **kwargs: str) -> "LabelledMetric":
        # Already bound; ignore kwargs and return self to satisfy chaining.
        return self # pragma: no cover

    def inc(self, amount: float = 1) -> None:
        if self._parent.kind != "counter":
            raise TypeError("inc() is only valid for counters") # pragma: no cover
        with self._parent._lock:
            current = self._parent._values.get(self._label_key, 0.0)
            self._parent._values[self._label_key] = current + float(amount)

    def set(self, value: float) -> None:
        if self._parent.kind != "gauge":
            raise TypeError("set() is only valid for gauges") # pragma: no cover
        with self._parent._lock:
            self._parent._values[self._label_key] = float(value)

    def observe(self, value: float) -> None:
        if self._parent.kind != "histogram":
            raise TypeError("observe() is only valid for histograms") # pragma: no cover
        v = float(value)
        with self._parent._lock:
            agg = self._parent._values.get(self._label_key)
            if agg is None:
                # Simple aggregation; extend with buckets if needed.
                agg = {"count": 0, "sum": 0.0, "min": v, "max": v}
            agg["count"] += 1
            agg["sum"] += v
            if v < agg["min"]:
                agg["min"] = v
            if v > agg["max"]:
                agg["max"] = v
            self._parent._values[self._label_key] = agg


class _InMemoryMetric(LabelledMetric):
    """
    Registry entry for a single metric (name+kind).
    `.labels(**kwargs)` returns a bound child that also conforms to LabelledMetric.
    """
    def __init__(self, name: str, label_names: list[str], kind: str):
        self.name = name
        self.label_names = label_names
        self.kind = kind  # "counter" | "gauge" | "histogram"
        self._values: Dict[LabelKey, Any] = {}
        self._lock = threading.Lock()

    def labels(self, **kwargs: str) -> LabelledMetric:
        label_key = _normalize_labels(self.name, kwargs)
        # Ensure slot exists for counters/gauges; histograms lazy-init on observe()
        if self.kind in ("counter", "gauge"):
            with self._lock:
                self._values.setdefault(label_key, 0.0)
        return _InMemoryMetricChild(self, label_key)

    # These three satisfy the protocol if someone calls methods directly
    # on the unbound metric (not recommended, but safe):
    def inc(self, amount: float = 1) -> None:
        raise TypeError("Call .labels(...).inc() on a counter metric") # pragma: no cover

    def set(self, value: float) -> None:
        raise TypeError("Call .labels(...).set() on a gauge metric") # pragma: no cover

    def observe(self, value: float) -> None:
        raise TypeError("Call .labels(...).observe() on a histogram metric") # pragma: no cover


class InMemoryMetrics(SpyMixin, Metrics):
    """
    In-memory metrics backend.
    Thread-safe, test-friendly; stores per-label values and provides snapshot helpers.
    """

    def __init__(self, logger: NoOpLogger | None = None):
        super().__init__(logger or NoOpLogger())
        self._snapshot_lock = threading.Lock()

    def create_metric(self, metric_name: str, label_names: list[str], kind: str) -> LabelledMetric:
        return _InMemoryMetric(metric_name, label_names, kind)

    # ----------------- Test helpers (read-only) -----------------

    def snapshot_counters(self) -> Dict[str, Dict[LabelKey, float]]:
        """
        { metric_name: { label_key: value } }
        """
        out: Dict[str, Dict[LabelKey, float]] = {}
        with self._snapshot_lock:
            reg = self._registries["counter"]
            for name, metric in reg.items():
                with metric._lock:
                    out[name] = dict(metric._values)
        return out

    def snapshot_gauges(self) -> Dict[str, Dict[LabelKey, float]]:
        out: Dict[str, Dict[LabelKey, float]] = {}
        with self._snapshot_lock:
            reg = self._registries["gauge"]
            for name, metric in reg.items():
                with metric._lock:
                    out[name] = dict(metric._values)
        return out

    def snapshot_histograms(self) -> Dict[str, Dict[LabelKey, Dict[str, float]]]:
        """
        { metric_name: { label_key: {"count": int, "sum": float, "min": float, "max": float} } }
        """
        out: Dict[str, Dict[LabelKey, Dict[str, float]]] = {}
        with self._snapshot_lock:
            reg = self._registries["histogram"]
            for name, metric in reg.items():
                with metric._lock:
                    out[name] = {k: dict(v) for k, v in metric._values.items()}
        return out

    def snapshot(self) -> Dict[str, Any]:
        """Unified snapshot for tools/tests/shells.

        Returns a dict with keys: counters, gauges, histograms.
        """
        return {
            "counters": self.snapshot_counters(),
            "gauges": self.snapshot_gauges(),
            "histograms": self.snapshot_histograms(),
        }
