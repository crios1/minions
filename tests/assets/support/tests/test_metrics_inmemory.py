import pytest

from minions._internal._framework.metric_definition import MetricDefinition
from minions._internal._framework.metrics_constants import (
    PIPELINE_EVENT_PRODUCED_TOTAL,
)
from tests.assets.support.metrics_inmemory import InMemoryMetrics


def test_snapshot_counter_value_total_across_label_sets():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="requests_total",
        kind="counter",
        label_names={"route"},
    )
    metrics.increment_counter(definition, labels={"route": "/a"}, amount=2)
    metrics.increment_counter(definition, labels={"route": "/b"}, amount=3)

    assert metrics.snapshot_counter_value_total("requests_total") == 5


def test_snapshot_counter_value_selects_label_set():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="requests_total",
        kind="counter",
        label_names={"route"},
    )
    metrics.increment_counter(definition, labels={"route": "/a"}, amount=2)
    metrics.increment_counter(definition, labels={"route": "/b"}, amount=3)

    assert metrics.snapshot_counter_value("requests_total", {"route": "/b"}) == 3


def test_snapshot_counter_value_raises_when_labels_are_not_found():
    metrics = InMemoryMetrics()
    metrics.increment_counter(
        MetricDefinition(
            name="requests_total",
            kind="counter",
            label_names={"route"},
        ),
        labels={"route": "/a"},
    )

    with pytest.raises(AssertionError, match="labels not found in snapshot"):
        metrics.snapshot_counter_value("requests_total", {"route": "/missing"})


def test_snapshot_gauge_value_total_across_label_sets():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="connections",
        kind="gauge",
        label_names={"host"},
    )
    metrics.set_gauge(definition, labels={"host": "a"}, value=4)
    metrics.set_gauge(definition, labels={"host": "b"}, value=5)

    assert metrics.snapshot_gauge_value_total("connections") == 9


def test_snapshot_gauge_value_selects_label_set():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="connections",
        kind="gauge",
        label_names={"host"},
    )
    metrics.set_gauge(definition, labels={"host": "a"}, value=4)
    metrics.set_gauge(definition, labels={"host": "b"}, value=5)

    assert metrics.snapshot_gauge_value("connections", {"host": "b"}) == 5


def test_snapshot_histogram_count_total_across_label_sets():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="latency_seconds",
        kind="histogram",
        label_names={"route"},
    )
    metrics.observe_histogram(definition, labels={"route": "/a"}, value=0.1)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.2)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.3)

    assert metrics.snapshot_histogram_count_total("latency_seconds") == 3


def test_snapshot_histogram_count_selects_label_set():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="latency_seconds",
        kind="histogram",
        label_names={"route"},
    )
    metrics.observe_histogram(definition, labels={"route": "/a"}, value=0.1)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.2)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.3)

    assert metrics.snapshot_histogram_count("latency_seconds", {"route": "/b"}) == 2


def test_snapshot_histogram_sum_total_across_label_sets():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="latency_seconds",
        kind="histogram",
        label_names={"route"},
    )
    metrics.observe_histogram(definition, labels={"route": "/a"}, value=0.1)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.2)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.3)

    assert metrics.snapshot_histogram_sum_total("latency_seconds") == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
        0.6,
        abs=1e-9,
    )


def test_snapshot_histogram_sum_selects_label_set():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name="latency_seconds",
        kind="histogram",
        label_names={"route"},
    )
    metrics.observe_histogram(definition, labels={"route": "/a"}, value=0.1)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.2)
    metrics.observe_histogram(definition, labels={"route": "/b"}, value=0.3)

    assert metrics.snapshot_histogram_sum(
        "latency_seconds",
        {"route": "/b"},
    ) == pytest.approx(0.5)  # pyright: ignore[reportUnknownMemberType]


def test_contract_assertion_rejects_different_label_names_for_declared_metric():
    metrics = InMemoryMetrics()
    definition = MetricDefinition(
        name=PIPELINE_EVENT_PRODUCED_TOTAL,
        kind="counter",
        label_names={"unexpected"},
    )
    metrics.increment_counter(definition, labels={"unexpected": "value"})

    assert (
        metrics.snapshot_counter_value(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            {"unexpected": "value"},
        )
        == 1
    )
    with pytest.raises(
        AssertionError,
        match=r"missing=\['pipeline'\] extra=\['unexpected'\]",
    ):
        metrics.assert_metric_label_observations_match_contract()


def test_contract_assertion_rejects_unknown_metric():
    metrics = InMemoryMetrics()
    metrics.increment_counter(
        MetricDefinition(
            name="unknown_counter_metric",
            kind="counter",
            label_names=set(),
        ),
        labels={},
    )

    with pytest.raises(
        AssertionError,
        match=r"unknown_counter_metric.*unknown_metric=True",
    ):
        metrics.assert_metric_label_observations_match_contract()

