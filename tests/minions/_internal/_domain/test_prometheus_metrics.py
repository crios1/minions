import asyncio
import contextlib
import re
import threading
import urllib.error
import urllib.request
from collections.abc import AsyncGenerator, Callable
from concurrent.futures import ThreadPoolExecutor
from typing import Literal, NoReturn, cast

import pytest

import minions._internal._framework.metrics_prometheus as metrics_prometheus_module
from minions._internal._framework.logger import Logger
from minions._internal._framework.logger_noop import NoOpLogger
from minions._internal._framework.metric_definition import Kind, MetricDefinition
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_MINION_WORKFLOW_STEP,
    LABEL_ORCHESTRATION_ID,
    LABEL_STATUS,
    MINION_WORKFLOW_STARTED_TOTAL,
    MINION_WORKFLOW_STEP_DURATION_SECONDS,
    STATE_STORE_PAYLOAD_SIZE_BYTES,
    SYSTEM_MEMORY_USED_PERCENT,
)
from minions._internal._framework.metrics_prometheus import PrometheusMetrics
from tests.assets.support.logger_inmemory import InMemoryLogger

PrometheusMetricsContextFactory = Callable[
    ..., contextlib.AbstractAsyncContextManager[PrometheusMetrics]
]


def _update_metric_by_kind(
    metrics: PrometheusMetrics,
    definition: MetricDefinition[Kind],
    *,
    labels: dict[str, str],
    value: float,
) -> None:
    if definition.kind == "counter":
        metrics.increment_counter(
            cast(MetricDefinition[Literal["counter"]], definition),
            labels=labels,
            amount=value,
        )
    elif definition.kind == "gauge":
        metrics.set_gauge(
            cast(MetricDefinition[Literal["gauge"]], definition),
            labels=labels,
            value=value,
        )
    elif definition.kind == "histogram":
        metrics.observe_histogram(
            cast(MetricDefinition[Literal["histogram"]], definition),
            labels=labels,
            value=value,
        )
    else:
        raise AssertionError(f"unhandled metric kind: {definition.kind}")


@pytest.fixture
def prometheus_metrics_context_factory() -> PrometheusMetricsContextFactory:
    @contextlib.asynccontextmanager
    async def _factory(
        *,
        logger: Logger,
        port: int,
    ) -> AsyncGenerator[PrometheusMetrics, None]:
        metrics = PrometheusMetrics(logger=logger, port=port)
        await metrics.startup()
        try:
            yield metrics
        finally:
            await metrics.shutdown()

    return _factory


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


def find_metric_value(text: str, name: str, labels: dict[str, str]) -> float:
    """Supported formats:
    - ``metric_name{label1="foo",label2="bar"} 42.0``
    - ``metric_name 42.0`` (without labels)
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
async def test_counter_exposed_on_http(
    prometheus_metrics_context_factory: PrometheusMetricsContextFactory,
):
    port = find_unused_port()
    async with prometheus_metrics_context_factory(logger=NoOpLogger(), port=port) as metrics:
        await metrics._mn_inc(
            MINION_WORKFLOW_STARTED_TOTAL,
            amount=1.0,
            labels={
                LABEL_ORCHESTRATION_ID: "o1",
                LABEL_MINION: "m1",
            },
        )

        page = await poll_read_metrics_from_http(port)
        value = find_metric_value(
            page,
            MINION_WORKFLOW_STARTED_TOTAL,
            {
                LABEL_ORCHESTRATION_ID: "o1",
                LABEL_MINION: "m1",
            },
        )
        assert value == 1.0


@pytest.mark.asyncio
async def test_prometheus_scrapes_metrics_recorded_before_startup():
    port = find_unused_port()
    metrics = PrometheusMetrics(logger=NoOpLogger(), port=port)

    workflow_started_labels = {
        LABEL_ORCHESTRATION_ID: "o1",
        LABEL_MINION: "m1",
    }
    workflow_step_duration_labels = {
        LABEL_ORCHESTRATION_ID: "o1",
        LABEL_MINION: "m1",
        LABEL_MINION_WORKFLOW_STEP: "s1",
        LABEL_STATUS: "succeeded",
    }

    await metrics._mn_inc(
        MINION_WORKFLOW_STARTED_TOTAL,
        amount=1.0,
        labels=workflow_started_labels,
    )
    await metrics._mn_set(SYSTEM_MEMORY_USED_PERCENT, value=25.0)
    await metrics._mn_observe(
        MINION_WORKFLOW_STEP_DURATION_SECONDS,
        value=0.75,
        labels=workflow_step_duration_labels,
    )

    try:
        await metrics.startup()

        page = await poll_read_metrics_from_http(port)

        assert find_metric_value(
            page,
            MINION_WORKFLOW_STARTED_TOTAL,
            workflow_started_labels
        ) == 1.0

        assert find_metric_value(
            page,
            SYSTEM_MEMORY_USED_PERCENT,
            {}
        ) == 25.0

        assert find_metric_value(
            page,
            MINION_WORKFLOW_STEP_DURATION_SECONDS + "_sum",
            workflow_step_duration_labels,
        ) == 0.75

        assert find_metric_value(
            page,
            MINION_WORKFLOW_STEP_DURATION_SECONDS + "_count",
            workflow_step_duration_labels,
        ) == 1.0

    finally:
        await metrics.shutdown()


@pytest.mark.asyncio
async def test_gauge_exposed_on_http(
    prometheus_metrics_context_factory: PrometheusMetricsContextFactory,
):
    port = find_unused_port()
    async with prometheus_metrics_context_factory(logger=NoOpLogger(), port=port) as metrics:
        await metrics._mn_set(SYSTEM_MEMORY_USED_PERCENT, value=42.5)

        page = await poll_read_metrics_from_http(port)
        value = find_metric_value(page, SYSTEM_MEMORY_USED_PERCENT, {})
        assert value == 42.5


@pytest.mark.asyncio
async def test_histogram_exposed_on_http(
    prometheus_metrics_context_factory: PrometheusMetricsContextFactory,
):
    port = find_unused_port()
    async with prometheus_metrics_context_factory(logger=NoOpLogger(), port=port) as metrics:
        await metrics._mn_observe(
            MINION_WORKFLOW_STEP_DURATION_SECONDS,
            value=0.75,
            labels={
                LABEL_ORCHESTRATION_ID: "o1",
                LABEL_MINION: "m1",
                LABEL_MINION_WORKFLOW_STEP: "s1",
                LABEL_STATUS: "succeeded",
            },
        )

        page = await poll_read_metrics_from_http(port)
        labels = {
            LABEL_ORCHESTRATION_ID: "o1",
            LABEL_MINION: "m1",
            LABEL_MINION_WORKFLOW_STEP: "s1",
            LABEL_STATUS: "succeeded",
        }

        sum_val = find_metric_value(page, MINION_WORKFLOW_STEP_DURATION_SECONDS + "_sum", labels)
        count_val = find_metric_value(
            page, MINION_WORKFLOW_STEP_DURATION_SECONDS + "_count", labels
        )

        assert count_val == 1.0
        assert sum_val == 0.75


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("metric_name", "value", "labels", "expected_bucket"),
    [
        (
            STATE_STORE_PAYLOAD_SIZE_BYTES,
            65_536.0,
            {"state_store_type": "SQLiteStateStore", "operation": "save_context"},
            "65536.0",
        ),
    ],
)
async def test_prometheus_uses_declared_histogram_bucket_boundaries(
    metric_name: str,
    value: float,
    labels: dict[str, str],
    expected_bucket: str,
):
    metrics = PrometheusMetrics(logger=NoOpLogger())
    await metrics._mn_observe(metric_name, value=value, labels=labels)

    family = next(
        family
        for family in metrics._collector_registry_state.collector_registry.collect()
        if family.name == metric_name
    )
    buckets = {
        sample.labels["le"]: sample.value
        for sample in family.samples
        if sample.name == f"{metric_name}_bucket"
    }
    assert buckets[expected_bucket] == 1.0


# Collector Registration and Snapshot Behavior


@pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
def test_repeated_operations_reuse_one_registered_metric_family(kind: Kind):
    metrics = PrometheusMetrics(logger=NoOpLogger())

    definition = MetricDefinition(
        name="metric",
        kind=kind,
        label_names=set(),
    )
    _update_metric_by_kind(metrics, definition, labels={}, value=1.0)
    _update_metric_by_kind(metrics, definition, labels={}, value=1.0)

    assert [
        family.name for family in metrics._collector_registry_state.collector_registry.collect()
    ] == [
        definition.name
    ]


def test_concurrent_prometheus_registration_publishes_one_collector():
    metrics = PrometheusMetrics(logger=NoOpLogger())
    definition = MetricDefinition(
        name="test_concurrent_registration",
        kind="gauge",
        label_names=set(),
    )
    worker_count = 8
    continue_together = threading.Barrier(worker_count + 1)  # workers and this test thread

    def record_metric(_index: int) -> None:
        continue_together.wait(timeout=5)
        metrics.set_gauge(definition, labels={}, value=1.0)

    with ThreadPoolExecutor(max_workers=worker_count) as executor:
        futures = [executor.submit(record_metric, index) for index in range(worker_count)]
        continue_together.wait(timeout=5)
        for future in futures:
            future.result(timeout=5)

    assert [
        family.name for family in metrics._collector_registry_state.collector_registry.collect()
    ] == [
        definition.name
    ]


# HTTP Server Lifecycle


@pytest.mark.asyncio
async def test_http_server_start_failure_logs_error(
    logger: InMemoryLogger,
    monkeypatch: pytest.MonkeyPatch,
):
    def raise_os_error(**_kwargs: object) -> NoReturn:
        raise OSError("address already in use")

    monkeypatch.setattr(
        metrics_prometheus_module,
        "start_http_server",
        raise_os_error,
    )
    metrics = PrometheusMetrics(logger=logger)

    with pytest.raises(OSError, match="address already in use"):
        await metrics.startup()

    assert logger.has_log("Failed to start metrics HTTP server")


@pytest.mark.asyncio
async def test_http_server_port_can_be_reused_by_new_metrics_instance_after_shutdown(
    logger: InMemoryLogger,
    prometheus_metrics_context_factory: PrometheusMetricsContextFactory,
):
    port = find_unused_port()

    async with prometheus_metrics_context_factory(logger=logger, port=port) as metrics:
        await metrics._mn_set(SYSTEM_MEMORY_USED_PERCENT, 42.5)
        assert metrics.snapshot()["gauge"][SYSTEM_MEMORY_USED_PERCENT] == [
            {"labels": {}, "value": 42.5}
        ]

    async with prometheus_metrics_context_factory(logger=logger, port=port) as metrics:
        assert not logger.has_log("Failed to start metrics HTTP server")
        await metrics._mn_set(SYSTEM_MEMORY_USED_PERCENT, 43.5)
        assert metrics.snapshot()["gauge"][SYSTEM_MEMORY_USED_PERCENT] == [
            {"labels": {}, "value": 43.5}
        ]
