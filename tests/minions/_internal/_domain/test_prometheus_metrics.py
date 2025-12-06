import pytest
import re
import time
import urllib.request

from prometheus_client import CollectorRegistry
from minions._internal._framework.metrics_prometheus import PrometheusMetrics
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import (
    MINION_WORKFLOW_STARTED_TOTAL,
    SYSTEM_MEMORY_USED_PERCENT,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
)

def read_metrics_from_http(port: int) -> str:
    with urllib.request.urlopen(f"http://localhost:{port}/metrics") as response:
        return response.read().decode()

def extract_metric_value(text: str, name: str, labels: dict[str, str]) -> float:
    """
    handles 'some_metric{label1="foo",label2="bar"} 42.0'
    and 'some_metric 42.0'
    """
    if labels:
        label_str = ",".join(f'{k}="{v}"' for k, v in labels.items())
        pattern = rf'^{re.escape(name)}{{{label_str}}} ([0-9\.e+-]+)$'
    else:
        pattern = rf'^{re.escape(name)} ([0-9\.e+-]+)$'

    for line in text.splitlines():
        match = re.match(pattern, line)
        if match:
            return float(match.group(1))

    raise KeyError(f"{name} with labels {labels} not found")

def find_unused_port():
    import socket
    with socket.socket() as s:
        s.bind(('', 0))
        return s.getsockname()[1]

# Success Cases

def test_counter_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    counter = metrics.create_metric(MINION_WORKFLOW_STARTED_TOTAL, ["minion"], "counter")
    counter.labels(minion="abc123").inc()
    time.sleep(0.1)  # give the HTTP server a moment

    page = read_metrics_from_http(port)
    value = extract_metric_value(page, MINION_WORKFLOW_STARTED_TOTAL, {"minion": "abc123"})
    assert value == 1.0

def test_gauge_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    gauge = metrics.create_metric(SYSTEM_MEMORY_USED_PERCENT, [], "gauge")
    gauge.set(42.5)
    time.sleep(0.1)

    page = read_metrics_from_http(port)
    value = extract_metric_value(page, SYSTEM_MEMORY_USED_PERCENT, {})
    assert value == 42.5

def test_histogram_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    histogram = metrics.create_metric(MINION_WORKFLOW_STEP_DURATION_SECONDS, ["minion", "minion_workflow_step"], "histogram")
    histogram.labels(minion="minion123", minion_workflow_step="step_xyz").observe(0.75)
    time.sleep(0.1)

    page = read_metrics_from_http(port)
    labels = {"minion": "minion123", "minion_workflow_step": "step_xyz"}

    sum_val = extract_metric_value(page, MINION_WORKFLOW_STEP_DURATION_SECONDS + "_sum", labels)
    count_val = extract_metric_value(page, MINION_WORKFLOW_STEP_DURATION_SECONDS + "_count", labels)

    assert count_val == 1.0
    assert sum_val == 0.75

# Failure Cases

def test_unknown_metric_kind_raises_value_error():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    with pytest.raises(ValueError) as exc_info:
        metrics.create_metric("invalid_metric", [], "bogus_kind")

    assert "Unknown metric kind" in str(exc_info.value)

@pytest.mark.asyncio
async def test_http_server_start_failure_logs_error():
    port = find_unused_port()
    registry = CollectorRegistry()

    PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    # Second instantiation should fail and trigger safe_create_task logging path
    PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    time.sleep(0.1) # Let logging task run
