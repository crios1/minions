import asyncio
import pytest
import re
import urllib.error
import urllib.request

from prometheus_client import CollectorRegistry
from minions._internal._framework.metrics_prometheus import PrometheusMetrics
from minions._internal._framework.logger_noop import NoOpLogger
from tests.assets.support.logger_inmemory import InMemoryLogger
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_COMPOSITE_KEY,
    LABEL_MINION_WORKFLOW_STEP,
    MINION_WORKFLOW_STARTED_TOTAL,
    SYSTEM_MEMORY_USED_PERCENT,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
)

def read_metrics_from_http(port: int) -> str:
    with urllib.request.urlopen(f"http://localhost:{port}/metrics") as response:
        return response.read().decode()


async def poll_read_metrics_from_http(
    port: int,
    *,
    timeout: float = 0.2,
    poll_interval: float = 0.01,
) -> str:
    deadline = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            return read_metrics_from_http(port)
        except urllib.error.URLError:
            if asyncio.get_running_loop().time() >= deadline:
                raise
            await asyncio.sleep(poll_interval)

def extract_metric_value(text: str, name: str, labels: dict[str, str]) -> float:
    """
    handles 'some_metric{label1="foo",label2="bar"} 42.0'
    and 'some_metric 42.0'
    """
    if labels:
        label_str = ",".join(f'{re.escape(k)}="{re.escape(v)}"' for k, v in labels.items())
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

@pytest.mark.asyncio
async def test_counter_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)
    await metrics.startup()

    counter = metrics.create_metric(MINION_WORKFLOW_STARTED_TOTAL, [LABEL_MINION_COMPOSITE_KEY], "counter")
    counter.labels(**{LABEL_MINION_COMPOSITE_KEY: "minion|config|pipeline"}).inc()

    page = await poll_read_metrics_from_http(port)
    value = extract_metric_value(
        page,
        MINION_WORKFLOW_STARTED_TOTAL,
        {LABEL_MINION_COMPOSITE_KEY: "minion|config|pipeline"},
    )
    assert value == 1.0

@pytest.mark.asyncio
async def test_gauge_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)
    await metrics.startup()

    gauge = metrics.create_metric(SYSTEM_MEMORY_USED_PERCENT, [], "gauge")
    gauge.set(42.5)

    page = await poll_read_metrics_from_http(port)
    value = extract_metric_value(page, SYSTEM_MEMORY_USED_PERCENT, {})
    assert value == 42.5

@pytest.mark.asyncio
async def test_histogram_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)
    await metrics.startup()

    histogram = metrics.create_metric(
        MINION_WORKFLOW_STEP_DURATION_SECONDS,
        [LABEL_MINION, LABEL_MINION_WORKFLOW_STEP],
        "histogram",
    )
    histogram.labels(**{LABEL_MINION: "minion123", LABEL_MINION_WORKFLOW_STEP: "step_xyz"}).observe(0.75)

    page = await poll_read_metrics_from_http(port)
    labels = {LABEL_MINION: "minion123", LABEL_MINION_WORKFLOW_STEP: "step_xyz"}

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
        metrics.create_metric("invalid_metric", [], "bogus_kind") # type: ignore

    assert "Unknown metric kind" in str(exc_info.value)

@pytest.mark.asyncio
async def test_http_server_start_failure_logs_error():
    port = find_unused_port()
    registry = CollectorRegistry()

    InMemoryLogger.enable_spy()
    InMemoryLogger.reset_spy()
    logger = InMemoryLogger()

    first = PrometheusMetrics(logger=logger, port=port, registry=registry)
    await first.startup()

    assert not logger.has_log("Failed to start metrics HTTP server")

    # Second instantiation should fail and trigger safe_create_task logging path
    second = PrometheusMetrics(logger=logger, port=port, registry=registry)
    await second.startup()

    assert logger.has_log("Failed to start metrics HTTP server")
