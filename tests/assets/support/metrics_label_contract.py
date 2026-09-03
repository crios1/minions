"""Validate metric label observations against the framework's declarations.

InMemoryMetrics records label usage and invokes this validation explicitly at a
test boundary so backend failures handled by framework code cannot hide contract
violations. Prometheus receives the same declared label names when its metrics
are created and rejects mismatched labels immediately when they are bound.
"""

from dataclasses import dataclass

from minions._internal._framework.metrics_constants import METRIC_LABEL_NAMES


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
