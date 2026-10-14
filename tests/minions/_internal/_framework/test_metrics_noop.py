from minions._internal._framework.metric_definition import MetricDefinition
from minions._internal._framework.metrics_noop import NoOpMetrics


def test_is_no_op():
    metrics = NoOpMetrics()

    metrics.increment_counter(
        MetricDefinition(
            name="request_count",
            kind="counter",
            label_names={"route"},
        ),
        labels={"route": "/orders"},
        amount=2.0,
    )
    metrics.set_gauge(
        MetricDefinition(
            name="active_requests",
            kind="gauge",
            label_names={"route"},
        ),
        labels={"route": "/orders"},
        value=3.0,
    )
    metrics.observe_histogram(
        MetricDefinition(
            name="request_duration_seconds",
            kind="histogram",
            label_names={"route"},
        ),
        labels={"route": "/orders"},
        value=0.5,
    )

    assert metrics.snapshot() == {"counter": {}, "gauge": {}, "histogram": {}}
