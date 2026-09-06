import asyncio
import re
import urllib.error
import urllib.request

import pytest
from prometheus_client import CollectorRegistry

from minions._internal._framework.logger import WARNING
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_ORCHESTRATION_ID,
    METRIC_LABEL_NAMES,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    SYSTEM_MEMORY_USED_PERCENT,
)
from minions._internal._framework.metrics_prometheus import PrometheusMetrics
from tests.assets.support.logger_inmemory import InMemoryLogger


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
    for line in text.splitlines():
        if labels:
            match = re.match(rf"^{re.escape(name)}{{([^}}]*)}} ([0-9\.e+-]+)$", line)
            if not match:
                continue
            actual_labels = dict(re.findall(r'([^=,]+)="([^"]*)"', match.group(1)))
            if actual_labels == labels:
                return float(match.group(2))
        else:
            match = re.match(rf"^{re.escape(name)} ([0-9\.e+-]+)$", line)
            if match:
                return float(match.group(1))

    raise KeyError(f"{name} with labels {labels} not found")


def find_unused_port():
    import socket

    with socket.socket() as s:
        s.bind(("", 0))
        return s.getsockname()[1]


# Metric Exposure


@pytest.mark.asyncio
async def test_counter_exposed_on_http():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)
    await metrics.startup()

    counter = metrics.create_metric(
        MINION_WORKFLOW_STARTED_TOTAL, [LABEL_ORCHESTRATION_ID, LABEL_MINION], "counter"
    )
    counter.labels(
        **{
            LABEL_ORCHESTRATION_ID: "dummy-orchestration-id",
            LABEL_MINION: "dummy-minion-id",
        }
    ).inc()

    page = await poll_read_metrics_from_http(port)
    value = extract_metric_value(
        page,
        MINION_WORKFLOW_STARTED_TOTAL,
        {
            LABEL_ORCHESTRATION_ID: "dummy-orchestration-id",
            LABEL_MINION: "dummy-minion-id",
        },
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
        [LABEL_ORCHESTRATION_ID, LABEL_MINION, LABEL_MINION_WORKFLOW_STEP],
        "histogram",
    )
    histogram.labels(
        **{
            LABEL_ORCHESTRATION_ID: "orchestration123",
            LABEL_MINION: "minion123",
            LABEL_MINION_WORKFLOW_STEP: "step_xyz",
        }
    ).observe(0.75)

    page = await poll_read_metrics_from_http(port)
    labels = {
        LABEL_ORCHESTRATION_ID: "orchestration123",
        LABEL_MINION: "minion123",
        LABEL_MINION_WORKFLOW_STEP: "step_xyz",
    }

    sum_val = extract_metric_value(page, MINION_WORKFLOW_STEP_DURATION_SECONDS + "_sum", labels)
    count_val = extract_metric_value(page, MINION_WORKFLOW_STEP_DURATION_SECONDS + "_count", labels)

    assert count_val == 1.0
    assert sum_val == 0.75


# Metric Registry Behavior


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
async def test_zero_label_metrics_update_without_unknown_warning(
    kind: str,
    monkeypatch: pytest.MonkeyPatch,
    logger: InMemoryLogger,
):
    metric_name = f"test_zero_label_{kind}"
    monkeypatch.setitem(METRIC_LABEL_NAMES, metric_name, [])
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=logger, registry=registry)

    if kind == "counter":
        await metrics._mn_inc(metric_name)
        assert metrics.snapshot_counters()[metric_name] == [{"labels": {}, "value": 1.0}]
    elif kind == "gauge":
        await metrics._mn_set(metric_name, 42.5)
        assert metrics.snapshot_gauges()[metric_name] == [{"labels": {}, "value": 42.5}]
    elif kind == "histogram":
        await metrics._mn_observe(metric_name, 0.75)
        assert {"labels": {}, "count": 1.0, "sum": 0.75} in metrics.snapshot_histograms()[
            metric_name
        ]
    else:
        raise Exception("unhandled metric kind")

    assert not logger.has_log(f"undeclared metric '{metric_name}'")


@pytest.mark.asyncio
async def test_undeclared_metric_warns_and_is_rejected(logger: InMemoryLogger):
    metric_name = "test_undeclared_metric"
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=logger, registry=registry)

    await metrics._mn_set(metric_name, 42.5, labels={"route": "/v1"})

    assert await logger.wait_for_log(f"undeclared metric '{metric_name}'", min_level=WARNING)
    undeclared_metric_logs = [
        log for log in logger.logs if f"undeclared metric '{metric_name}'" in log.msg
    ]
    assert len(undeclared_metric_logs) == 1
    assert undeclared_metric_logs[0].level == WARNING
    assert "no declared label schema" in undeclared_metric_logs[0].msg
    assert metric_name not in metrics.snapshot_gauges()


# Failure Cases


def test_unknown_metric_kind_raises_value_error():
    port = find_unused_port()
    registry = CollectorRegistry()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port, registry=registry)

    with pytest.raises(ValueError) as exc_info:
        metrics.create_metric("invalid_metric", [], "bogus_kind")  # type: ignore

    assert "Unknown metric kind" in str(exc_info.value)


@pytest.mark.asyncio
async def test_http_server_start_failure_logs_error(logger: InMemoryLogger):
    port = find_unused_port()
    registry = CollectorRegistry()

    first = PrometheusMetrics(logger=logger, port=port, registry=registry)
    await first.startup()

    assert not logger.has_log("Failed to start metrics HTTP server")

    # Second instantiation should fail and trigger safe_create_task logging path
    second = PrometheusMetrics(logger=logger, port=port, registry=registry)
    await second.startup()

    assert logger.has_log("Failed to start metrics HTTP server")
