from dataclasses import dataclass
from math import isnan
from typing import Generic, Literal, TypeVar

Kind = Literal["counter", "gauge", "histogram"]
MetricKind = TypeVar("MetricKind", bound=Kind, covariant=True)

_VALID_KINDS = frozenset({"counter", "gauge", "histogram"})


@dataclass(frozen=True, slots=True)
class MetricDefinition(Generic[MetricKind]):
    name: str
    kind: MetricKind
    label_names: tuple[str, ...]
    histogram_buckets: tuple[float, ...] | None = None

    def __init__(
        self,
        name: str,
        kind: MetricKind,
        label_names: set[str],
        histogram_buckets: tuple[float, ...] | None = None,
    ) -> None:
        if not name:
            raise ValueError("Metric names must be non-empty")
        if kind not in _VALID_KINDS:
            raise ValueError(f"Unknown metric kind: {kind!r}")

        normalized_label_names = tuple(sorted(label_names))
        if any(not name for name in normalized_label_names):
            raise ValueError("Metric label names must be non-empty strings")
        if any(name.startswith("__") for name in normalized_label_names):
            raise ValueError("Metric label names cannot start with '__'")
        if any(not name.isidentifier() for name in normalized_label_names):
            raise ValueError("Metric label names must satisfy Python identifier naming rules")

        if kind != "histogram" and histogram_buckets is not None:
            raise ValueError("Only histogram metrics may define histogram buckets")

        normalized_buckets = None
        if histogram_buckets is not None:
            normalized_buckets = tuple(float(bucket) for bucket in histogram_buckets)
            if any(isnan(bucket) for bucket in normalized_buckets):
                raise ValueError("Histogram buckets cannot contain NaN")
            if any(
                a >= b for a, b in zip(normalized_buckets, normalized_buckets[1:])
            ):
                raise ValueError("Histogram buckets must be strictly increasing")

        object.__setattr__(self, "name", name)
        object.__setattr__(self, "kind", kind)
        object.__setattr__(self, "label_names", normalized_label_names)
        object.__setattr__(self, "histogram_buckets", normalized_buckets)
