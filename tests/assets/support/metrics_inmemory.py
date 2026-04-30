import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Tuple, TypeVar, cast

from minions._internal._framework.metrics import (
    CounterSample,
    GaugeSample,
    HistogramSample,
    Kind,
    SnapshotCounters,
    SnapshotGauges,
    SnapshotHistograms,
    SnapshotResult,
)
from minions._internal._framework.metrics_interface import LabelledMetric
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import METRIC_LABEL_NAMES

from .metrics_spied import SpiedMetrics


LabelKey = Tuple[Tuple[str, str], ...]  # sorted (name, value) pairs for hashing


SampleT = TypeVar("SampleT", CounterSample, GaugeSample, HistogramSample)


@dataclass(frozen=True)
class MetricLabelEmission:
    metric_name: str
    labels: frozenset[str]


@dataclass(frozen=True)
class MetricLabelContractViolation:
    metric_name: str
    expected: frozenset[str]
    actual: frozenset[str]
    unknown_metric: bool = False

    @property
    def missing(self) -> frozenset[str]:
        return self.expected - self.actual

    @property
    def extra(self) -> frozenset[str]:
        return self.actual - self.expected


def validate_metric_label_contract(
    metric_name: str,
    labels: frozenset[str],
) -> MetricLabelContractViolation | None:
    if metric_name not in METRIC_LABEL_NAMES:
        return MetricLabelContractViolation(
            metric_name=metric_name,
            expected=frozenset(),
            actual=labels,
            unknown_metric=True,
        )
    expected = frozenset(METRIC_LABEL_NAMES.get(metric_name, []))
    if expected == labels:
        return None
    return MetricLabelContractViolation(
        metric_name=metric_name,
        expected=expected,
        actual=labels,
    )


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
    def __init__(
        self,
        name: str,
        label_names: list[str],
        kind: Kind,
        record_metric_labels: Callable[[MetricLabelEmission], None],
    ):
        self.name = name
        self.label_names = label_names
        self.kind = kind
        self._record_metric_labels = record_metric_labels
        self._values: Dict[LabelKey, Any] = {}
        self._lock = threading.Lock()

    def labels(self, **kwargs: str) -> LabelledMetric:
        self._record_metric_labels(
            MetricLabelEmission(
                metric_name=self.name,
                labels=frozenset(kwargs),
            )
        )
        label_key = _normalize_labels(self.name, kwargs)
        # Ensure slot exists for counters/gauges; histograms lazy-init on observe()
        if self.kind in ("counter", "gauge"):
            with self._lock:
                self._values.setdefault(label_key, 0.0)
        return _InMemoryMetricChild(self, label_key)

    def snapshot_values(self) -> Dict[LabelKey, Any]:
        with self._lock:
            if self.kind == "histogram":
                return {k: dict(v) for k, v in self._values.items()}
            return dict(self._values)

    # These three satisfy the protocol if someone calls methods directly
    # on the unbound metric (not recommended, but safe):
    def inc(self, amount: float = 1) -> None:
        raise TypeError("Call .labels(...).inc() on a counter metric") # pragma: no cover

    def set(self, value: float) -> None:
        raise TypeError("Call .labels(...).set() on a gauge metric") # pragma: no cover

    def observe(self, value: float) -> None:
        raise TypeError("Call .labels(...).observe() on a histogram metric") # pragma: no cover


class InMemoryMetrics(SpiedMetrics):
    """
    In-memory metrics backend.
    Thread-safe, test-friendly; stores per-label values and provides snapshot helpers.
    """
    def __init__(self, logger: NoOpLogger | None = None):
        super().__init__(logger or NoOpLogger())
        self._snapshot_lock = threading.Lock()
        self._metric_label_emissions: list[MetricLabelEmission] = []
        self._metric_label_emissions_lock = threading.Lock()

    def create_metric(self, metric_name: str, label_names: list[str], kind: Kind) -> LabelledMetric:
        return _InMemoryMetric(
            metric_name,
            label_names,
            kind,
            self._record_metric_labels,
        )

    # ----------------- Test helpers (read-only) -----------------

    def _record_metric_labels(
        self,
        emission: MetricLabelEmission,
    ) -> None:
        with self._metric_label_emissions_lock:
            self._metric_label_emissions.append(emission)

    def metric_label_emissions(self) -> list[MetricLabelEmission]:
        with self._metric_label_emissions_lock:
            return list(self._metric_label_emissions)

    def clear_metric_label_emissions(self) -> None:
        with self._metric_label_emissions_lock:
            self._metric_label_emissions.clear()

    def assert_recorded_labels_match_contract(self) -> None:
        violations = [
            violation
            for emission in self.metric_label_emissions()
            if (
                violation := validate_metric_label_contract(
                    emission.metric_name,
                    emission.labels,
                )
            ) is not None
        ]
        if not violations:
            return
        details = "\n".join(
            (
                f"{v.metric_name}: expected={sorted(v.expected)!r} "
                f"actual={sorted(v.actual)!r} "
                f"missing={sorted(v.missing)!r} extra={sorted(v.extra)!r} "
                f"unknown_metric={v.unknown_metric!r}"
            )
            for v in violations
        )
        raise AssertionError(f"metric label contract violations:\n{details}")

    @staticmethod
    def find_sample(
        samples: list[SampleT],
        labels: dict[str, str],
    ) -> SampleT:
        for sample in samples:
            if sample["labels"] == labels:
                return sample
        raise AssertionError(f"labels not found in snapshot: {labels}")

    def snapshot_counters(self) -> SnapshotCounters:
        out: SnapshotCounters = {}
        with self._snapshot_lock:
            reg = self._mn_registries["counter"]
            for name, metric in reg.items():
                metric_impl = cast(_InMemoryMetric, metric)
                out[name] = [
                    {"labels": dict(label_key), "value": float(value)}
                    for label_key, value in metric_impl.snapshot_values().items()
                ]
        return out

    def snapshot_gauges(self) -> SnapshotGauges:
        out: SnapshotGauges = {}
        with self._snapshot_lock:
            reg = self._mn_registries["gauge"]
            for name, metric in reg.items():
                metric_impl = cast(_InMemoryMetric, metric)
                out[name] = [
                    {"labels": dict(label_key), "value": float(value)}
                    for label_key, value in metric_impl.snapshot_values().items()
                ]
        return out

    def snapshot_histograms(self) -> SnapshotHistograms:
        out: SnapshotHistograms = {}
        with self._snapshot_lock:
            reg = self._mn_registries["histogram"]
            for name, metric in reg.items():
                metric_impl = cast(_InMemoryMetric, metric)
                out[name] = [
                    {
                        "labels": dict(label_key),
                        "count": float(agg["count"]),
                        "sum": float(agg["sum"]),
                    }
                    for label_key, agg in metric_impl.snapshot_values().items()
                ]
        return out

    def snapshot(self) -> SnapshotResult:
        """Unified snapshot for tools/tests/shells.

        Returns a dict with keys: counter, gauge, histogram.
        """
        return {
            "counter": self.snapshot_counters(),
            "gauge": self.snapshot_gauges(),
            "histogram": self.snapshot_histograms(),
        }
