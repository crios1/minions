from abc import abstractmethod
from collections.abc import Mapping
from typing import Literal, TypedDict, cast

from .logger import Logger
from .logger_backed_async_component import LoggerBackedAsyncComponent
from .metric_definition import Kind, MetricDefinition
from .metrics_constants import METRIC_DEFINITIONS


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


class SnapshotResult(TypedDict):
    counter: SnapshotCounters
    gauge: SnapshotGauges
    histogram: SnapshotHistograms


class Metrics(LoggerBackedAsyncComponent):
    _mn_user_facing = True

    def __init__(self, logger: Logger):
        super().__init__(logger)

    def _mn_get_metric_definition(self, metric_name: str) -> MetricDefinition[Kind]:
        definition = METRIC_DEFINITIONS.get(metric_name)
        if definition is not None:
            return definition

        raise ValueError(f"Undeclared metric '{metric_name}': no declared definition")

    def _mn_validate_metric_labels(
        self,
        definition: MetricDefinition[Kind],
        labels: Mapping[str, str] | None,
    ) -> None:
        expected = frozenset(definition.label_names)
        actual = frozenset((labels or {}).keys())
        if actual == expected:
            return

        raise ValueError(
            f"Metric '{definition.name}' label keys do not match declared schema: "
            f"expected={sorted(expected)!r} actual={sorted(actual)!r} "
            f"missing={sorted(expected - actual)!r} extra={sorted(actual - expected)!r}"
        )

    @staticmethod
    def _mn_require_metric_kind(
        definition: MetricDefinition[Kind],
        expected_kind: Kind,
    ) -> None:
        if definition.kind != expected_kind:
            raise ValueError(
                f"Metric '{definition.name}' is declared as kind '{definition.kind}', "
                f"but the operation requested '{expected_kind}'"
            )

    @staticmethod
    def _mn_require_same_definition_for_metric_name(
        registered_definition: MetricDefinition[Kind],
        requested_definition: MetricDefinition[Kind],
    ) -> None:
        if registered_definition == requested_definition:
            return

        def describe(definition: MetricDefinition[Kind]) -> str:
            return (
                f"definition(kind={definition.kind!r}, "
                f"label_names={list(definition.label_names)!r}, "
                f"histogram_buckets={definition.histogram_buckets!r})"
            )

        raise ValueError(
            f"Metric name '{requested_definition.name}' is already associated with "
            f"{describe(registered_definition)}; requested "
            f"{describe(requested_definition)}"
        )

    def _mn_inc_unsafe(
        self,
        metric_name: str,
        amount: float = 1,
        labels: Mapping[str, str] | None = None,
    ):
        definition = self._mn_get_metric_definition(metric_name)
        self._mn_validate_metric_labels(definition, labels)
        self._mn_require_metric_kind(definition, "counter")
        self.increment_counter(
            cast(MetricDefinition[Literal["counter"]], definition),
            labels=labels or {},
            amount=amount,
        )

    def _mn_set_unsafe(
        self,
        metric_name: str,
        value: float,
        labels: Mapping[str, str] | None = None,
    ):
        definition = self._mn_get_metric_definition(metric_name)
        self._mn_validate_metric_labels(definition, labels)
        self._mn_require_metric_kind(definition, "gauge")
        self.set_gauge(
            cast(MetricDefinition[Literal["gauge"]], definition),
            labels=labels or {},
            value=value,
        )

    def _mn_observe_unsafe(
        self,
        metric_name: str,
        value: float,
        labels: Mapping[str, str] | None = None,
    ):
        definition = self._mn_get_metric_definition(metric_name)
        self._mn_validate_metric_labels(definition, labels)
        self._mn_require_metric_kind(definition, "histogram")
        self.observe_histogram(
            cast(MetricDefinition[Literal["histogram"]], definition),
            labels=labels or {},
            value=value,
        )

    async def _mn_inc(
        self,
        metric_name: str,
        amount: float = 1,
        labels: Mapping[str, str] | None = None,
    ):
        """Increment a counter by the given amount (positive or negative)."""
        return await self._mn_safe_run_and_log_failure(
            method=self._mn_inc_unsafe,
            method_args=[metric_name, amount, labels],
            log_msg=(
                f"{type(self).__module__}.{type(self).__qualname__}._mn_inc failed"
            ),
        )

    async def _mn_set(
        self,
        metric_name: str,
        value: float,
        labels: Mapping[str, str] | None = None,
    ):
        """Set a gauge to a specific value."""
        return await self._mn_safe_run_and_log_failure(
            method=self._mn_set_unsafe,
            method_args=[metric_name, value, labels],
            log_msg=(
                f"{type(self).__module__}.{type(self).__qualname__}._mn_set failed"
            ),
        )

    async def _mn_observe(
        self,
        metric_name: str,
        value: float,
        labels: Mapping[str, str] | None = None,
    ):
        """Observe a value in a histogram."""
        return await self._mn_safe_run_and_log_failure(
            method=self._mn_observe_unsafe,
            method_args=[metric_name, value, labels],
            log_msg=(
                f"{type(self).__module__}.{type(self).__qualname__}._mn_observe failed"
            ),
        )

    async def _mn_snapshot(self) -> SnapshotResult | None:
        """Return a snapshot or ``None`` if the backend snapshot fails."""
        return await self._mn_safe_run_and_log_failure(self.snapshot)

    @abstractmethod
    def increment_counter(
        self,
        definition: MetricDefinition[Literal["counter"]],
        *,
        labels: Mapping[str, str],
        amount: float = 1,
    ) -> None:
        """Atomically record a counter increment for ``definition``."""

    @abstractmethod
    def set_gauge(
        self,
        definition: MetricDefinition[Literal["gauge"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        """Atomically record a gauge value for ``definition``."""

    @abstractmethod
    def observe_histogram(
        self,
        definition: MetricDefinition[Literal["histogram"]],
        *,
        labels: Mapping[str, str],
        value: float,
    ) -> None:
        """Atomically record a histogram observation for ``definition``."""

    @abstractmethod
    def snapshot(self) -> SnapshotResult:
        """Return a coherent snapshot of all metric kinds.

        Histogram samples expose their labels, observation counts, and sums.
        Bucket-level distributions are outside the generic snapshot contract.
        """
