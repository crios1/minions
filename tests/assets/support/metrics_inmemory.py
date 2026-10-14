import threading
from collections.abc import Generator, Iterable, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any, Literal, TypeVar

from minions._internal._framework.logger import Logger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metric_definition import Kind, MetricDefinition
from minions._internal._framework.metrics import (
    CounterSample,
    GaugeSample,
    HistogramSample,
    SnapshotCounters,
    SnapshotGauges,
    SnapshotHistograms,
    SnapshotResult,
)
from minions._internal._framework.metrics_constants import METRIC_DEFINITIONS

from .metrics_spied import SpiedMetrics

_LabelSetKey = tuple[tuple[str, str], ...]  # sorted (name, value) pairs for hashing
_MetricValuesByLabelSet = dict[_LabelSetKey, Any]

_SampleT = TypeVar("_SampleT", CounterSample, GaugeSample, HistogramSample)


@dataclass(frozen=True, slots=True)
class _MetricRecord:
    definition: MetricDefinition[Kind]
    values_by_label_set: _MetricValuesByLabelSet


@dataclass(frozen=True)
class _MetricLabelObservation:
    metric_name: str
    labels: frozenset[str]

@dataclass(frozen=True)
class _MetricLabelContractViolation:
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


def _validate_metric_label_contract(
    metric_name: str,
    labels: frozenset[str],
) -> _MetricLabelContractViolation | None:
    definition = METRIC_DEFINITIONS.get(metric_name)
    if definition is None:
        return _MetricLabelContractViolation(
            metric_name=metric_name,
            expected=frozenset(),
            actual=labels,
            unknown_metric=True,
        )
    expected = frozenset(definition.label_names)
    if expected != labels:
        return _MetricLabelContractViolation(
            metric_name=metric_name,
            expected=expected,
            actual=labels,
        )
    return None


def _find_sample(
    samples: list[_SampleT],
    labels: dict[str, str],
) -> _SampleT:
    for sample in samples:
        if sample["labels"] == labels:
            return sample
    raise AssertionError(f"labels not found in snapshot: {labels}")


def _normalized_label_set_key(
    label_names: Iterable[str],
    labels: Mapping[str, str],
) -> _LabelSetKey:
    if label_names:
        items = tuple((name, str(labels[name])) for name in label_names)
    else:
        items = tuple(sorted((k, str(v)) for k, v in labels.items()))
    return items


class InMemoryMetrics(SpiedMetrics):
    """Thread-safe in-memory metrics backend for tests.

    Its state is process-owned test state rather than a native resource, so
    the inherited lifecycle hooks leave definitions and samples
    available across startup and shutdown.
    """

    def __init__(
        self,
        logger: Logger | None = None,
    ):
        super().__init__(logger or NoOpLogger())
        self._metric_label_observations: list[_MetricLabelObservation] = []
        self._metric_label_observations_lock = threading.Lock()
        self._metric_records_lock = threading.Lock()
        self._metric_records: dict[str, _MetricRecord] = {}

    @contextmanager
    def _metric_values_for_update(
        self,
        definition: MetricDefinition[Kind],
    ) -> Generator[_MetricValuesByLabelSet, None, None]:
        with self._metric_records_lock:
            record = self._metric_records.get(definition.name)
            if record:
                self._mn_require_same_definition_for_metric_name(
                    record.definition,
                    definition,
                )
                metric_values = record.values_by_label_set
            else:
                metric_values: _MetricValuesByLabelSet = {}
                self._metric_records[definition.name] = _MetricRecord(
                    definition,
                    metric_values,
                )
            yield metric_values

    def _record_metric_label_observation(
        self,
        observation: _MetricLabelObservation,
    ) -> None:
        with self._metric_label_observations_lock:
            self._metric_label_observations.append(observation)


    def increment_counter(
        self,
        definition: MetricDefinition[Literal["counter"]],
        *,
        labels: Mapping[str, str],
        amount: float = 1,
    ) -> None:
        key = _normalized_label_set_key(definition.label_names, labels)
        with self._metric_values_for_update(definition) as metric_values:
            self._record_metric_label_observation(
                _MetricLabelObservation(definition.name, frozenset(labels))
            )
            current = metric_values.get(key, 0.0)
            metric_values[key] = current + float(amount)

    def set_gauge(
        self,
        definition: MetricDefinition[Literal["gauge"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        key = _normalized_label_set_key(definition.label_names, labels)
        with self._metric_values_for_update(definition) as metric_values:
            self._record_metric_label_observation(
                _MetricLabelObservation(definition.name, frozenset(labels))
            )
            metric_values[key] = float(value)

    def observe_histogram(
        self,
        definition: MetricDefinition[Literal["histogram"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        key = _normalized_label_set_key(definition.label_names, labels)
        with self._metric_values_for_update(definition) as metric_values:
            self._record_metric_label_observation(
                _MetricLabelObservation(definition.name, frozenset(labels))
            )
            v = float(value)
            aggregate = metric_values.get(key)
            if aggregate is None:
                aggregate = {"count": 0, "sum": 0.0, "min": v, "max": v}
            aggregate["count"] += 1
            aggregate["sum"] += v
            if v < aggregate["min"]:
                aggregate["min"] = v
            if v > aggregate["max"]:
                aggregate["max"] = v
            metric_values[key] = aggregate

    # ----------------- Test helpers -----------------

    def snapshot(self) -> SnapshotResult:
        with self._metric_records_lock:
            counters: SnapshotCounters = {}
            gauges: SnapshotGauges = {}
            histograms: SnapshotHistograms = {}
            for record in self._metric_records.values():
                metric_values = record.values_by_label_set
                if record.definition.kind == "counter":
                    counters[record.definition.name] = [
                        {"labels": dict(key), "value": float(value)}
                        for key, value in metric_values.items()
                    ]
                elif record.definition.kind == "gauge":
                    gauges[record.definition.name] = [
                        {"labels": dict(key), "value": float(value)}
                        for key, value in metric_values.items()
                    ]
                else:
                    histograms[record.definition.name] = [
                        {
                            "labels": dict(key),
                            "count": float(aggregate["count"]),
                            "sum": float(aggregate["sum"]),
                        }
                        for key, aggregate in metric_values.items()
                    ]
            return {"counter": counters, "gauge": gauges, "histogram": histograms}

    def snapshot_counter_value_total(self, metric_name: str) -> float:
        snapshot = self.snapshot()
        return sum(sample["value"] for sample in snapshot["counter"].get(metric_name, []))

    def snapshot_counter_value(self, metric_name: str, labels: dict[str, str]) -> float:
        snapshot = self.snapshot()
        return _find_sample(snapshot["counter"][metric_name], labels)["value"]

    def snapshot_gauge_value_total(self, metric_name: str) -> float:
        snapshot = self.snapshot()
        return sum(sample["value"] for sample in snapshot["gauge"].get(metric_name, []))

    def snapshot_gauge_value(self, metric_name: str, labels: dict[str, str]) -> float:
        snapshot = self.snapshot()
        return _find_sample(snapshot["gauge"][metric_name], labels)["value"]

    def snapshot_histogram_count_total(self, metric_name: str) -> float:
        snapshot = self.snapshot()
        return sum(sample["count"] for sample in snapshot["histogram"].get(metric_name, []))

    def snapshot_histogram_count(self, metric_name: str, labels: dict[str, str]) -> float:
        snapshot = self.snapshot()
        return _find_sample(snapshot["histogram"][metric_name], labels)["count"]

    def snapshot_histogram_sum_total(self, metric_name: str) -> float:
        snapshot = self.snapshot()
        return sum(sample["sum"] for sample in snapshot["histogram"].get(metric_name, []))

    def snapshot_histogram_sum(self, metric_name: str, labels: dict[str, str]) -> float:
        snapshot = self.snapshot()
        return _find_sample(snapshot["histogram"][metric_name], labels)["sum"]

    def assert_metric_label_observations_match_contract(self) -> None:
        with self._metric_label_observations_lock:
            observations = tuple(self._metric_label_observations)
        violations = [
            violation
            for observation in observations
            if (
                violation := _validate_metric_label_contract(
                    observation.metric_name,
                    observation.labels,
                )
            )
            is not None
        ]
        if not violations:
            return
        details = "\n".join(
            (
                f"{violation.metric_name}: expected={sorted(violation.expected)!r} "
                f"actual={sorted(violation.actual)!r} "
                f"missing={sorted(violation.missing)!r} "
                f"extra={sorted(violation.extra)!r} "
                f"unknown_metric={violation.unknown_metric!r}"
            )
            for violation in violations
        )
        raise AssertionError(f"metric label contract violations:\n{details}")
