import asyncio

import pytest

from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_OPERATION,
    LABEL_ORCHESTRATION_ID,
    LABEL_PIPELINE,
    LABEL_STATE_STORE_TYPE,
    METRIC_LABEL_NAMES,
    MINION_WORKFLOW_INFLIGHT_GAUGE,
    PIPELINE_EVENT_PRODUCED_TOTAL,
    STATE_STORE_OPERATION_DURATION_SECONDS,
)
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class TestInMemoryMetrics:
    def test_counter_healthy_with_label_ordering(self):
        """
        Counter increments aggregate correctly; labels follow the supplied schema order.
        """
        m = InMemoryMetrics()
        counter = m.create_metric("my_counter", ["pipeline", "minion"], "counter")
        counter.labels(minion="m1", pipeline="p1").inc(2)
        counter.labels(minion="m1", pipeline="p1").inc()  # +1

        samples = m.snapshot_counters()["my_counter"]
        assert len(samples) == 1
        sample = samples[0]
        assert sample["labels"] == {"pipeline": "p1", "minion": "m1"}
        assert list(sample["labels"].keys()) == ["pipeline", "minion"]
        assert sample["value"] == 3.0

    def test_gauge_set_with_missing_label_defaults(self):
        """
        Gauge .set() overwrites, and missing expected labels default to "".
        """
        m = InMemoryMetrics()
        gauge = m.create_metric("cpu_gauge", ["region"], "gauge")
        gauge.labels().set(10.5)  # region defaults to ""
        gauge.labels(region="us-east").set(7.0)
        gauge.labels(region="us-east").set(8.0)  # overwrite

        assert m.snapshot_gauge_value("cpu_gauge", {"region": ""}) == 10.5
        assert m.snapshot_gauge_value("cpu_gauge", {"region": "us-east"}) == 8.0

    def test_label_mismatch_is_rejected_only_when_contract_is_asserted(
        self,
    ):
        metrics = InMemoryMetrics()
        counter = metrics.create_metric(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            [LABEL_PIPELINE],
            "counter",
        )

        counter.labels(unexpected="value").inc()

        assert metrics.snapshot_counter_value(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            {LABEL_PIPELINE: ""},
        ) == 1
        with pytest.raises(
            AssertionError,
            match=r"missing=\['pipeline'\] extra=\['unexpected'\]",
        ):
            metrics.assert_metric_label_observations_match_contract()

    def test_histogram_observe_aggregates(self):
        """
        Histogram aggregates count/sum/min/max for a label set.
        """
        m = InMemoryMetrics()
        h = m.create_metric("latency_seconds", ["route"], "histogram")
        h.labels(route="/v1/foo").observe(0.120)
        h.labels(route="/v1/foo").observe(0.080)
        h.labels(route="/v1/foo").observe(0.200)

        samples = m.snapshot_histograms()["latency_seconds"]
        stats = InMemoryMetrics.find_sample(samples, {"route": "/v1/foo"})
        assert stats["count"] == 3.0
        assert stats["sum"] == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
            0.400,
            abs=1e-9,
        )

    def test_snapshot_counter_value_total_across_label_sets(
        self,
    ):
        metrics = InMemoryMetrics()
        counter = metrics.create_metric("requests_total", ["route"], "counter")
        counter.labels(route="/a").inc(2)
        counter.labels(route="/b").inc(3)

        assert metrics.snapshot_counter_value_total("requests_total") == 5

    def test_snapshot_counter_value_selects_label_set(
        self,
    ):
        metrics = InMemoryMetrics()
        counter = metrics.create_metric("requests_total", ["route"], "counter")
        counter.labels(route="/a").inc(2)
        counter.labels(route="/b").inc(3)

        assert metrics.snapshot_counter_value("requests_total", {"route": "/b"}) == 3

    def test_snapshot_gauge_value_total_across_label_sets(
        self,
    ):
        metrics = InMemoryMetrics()
        gauge = metrics.create_metric("connections", ["host"], "gauge")
        gauge.labels(host="a").set(4)
        gauge.labels(host="b").set(5)

        assert metrics.snapshot_gauge_value_total("connections") == 9

    def test_snapshot_gauge_value_selects_label_set(
        self,
    ):
        metrics = InMemoryMetrics()
        gauge = metrics.create_metric("connections", ["host"], "gauge")
        gauge.labels(host="a").set(4)
        gauge.labels(host="b").set(5)

        assert metrics.snapshot_gauge_value("connections", {"host": "b"}) == 5

    def test_snapshot_histogram_count_total_across_label_sets(
        self,
    ):
        metrics = InMemoryMetrics()
        histogram = metrics.create_metric("latency_seconds", ["route"], "histogram")
        histogram.labels(route="/a").observe(0.1)
        histogram.labels(route="/b").observe(0.2)
        histogram.labels(route="/b").observe(0.3)

        assert metrics.snapshot_histogram_count_total("latency_seconds") == 3

    def test_snapshot_histogram_count_selects_label_set(
        self,
    ):
        metrics = InMemoryMetrics()
        histogram = metrics.create_metric("latency_seconds", ["route"], "histogram")
        histogram.labels(route="/a").observe(0.1)
        histogram.labels(route="/b").observe(0.2)
        histogram.labels(route="/b").observe(0.3)

        assert metrics.snapshot_histogram_count("latency_seconds", {"route": "/b"}) == 2

    def test_snapshot_histogram_sum_total_across_label_sets(
        self,
    ):
        metrics = InMemoryMetrics()
        histogram = metrics.create_metric("latency_seconds", ["route"], "histogram")
        histogram.labels(route="/a").observe(0.1)
        histogram.labels(route="/b").observe(0.2)
        histogram.labels(route="/b").observe(0.3)

        assert metrics.snapshot_histogram_sum_total("latency_seconds") == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
            0.6,
            abs=1e-9,
        )

    def test_snapshot_histogram_sum_selects_label_set(
        self,
    ):
        metrics = InMemoryMetrics()
        histogram = metrics.create_metric("latency_seconds", ["route"], "histogram")
        histogram.labels(route="/a").observe(0.1)
        histogram.labels(route="/b").observe(0.2)
        histogram.labels(route="/b").observe(0.3)

        assert metrics.snapshot_histogram_sum(
            "latency_seconds",
            {"route": "/b"},
        ) == pytest.approx(0.5)  # pyright: ignore[reportUnknownMemberType]

    def test_snapshot_counter_value_raises_when_labels_are_not_found(
        self,
    ):
        metrics = InMemoryMetrics()
        counter = metrics.create_metric("requests_total", ["route"], "counter")
        counter.labels(route="/a").inc()

        with pytest.raises(AssertionError, match="labels not found in snapshot"):
            metrics.snapshot_counter_value("requests_total", {"route": "/missing"})

    def test_unbound_metric_methods_raise(self):
        """
        Calling .inc/.set/.observe on the unbound metric (without .labels()) should raise TypeError.
        """
        m = InMemoryMetrics()

        ctr = m.create_metric("events_total", ["minion"], "counter")
        with pytest.raises(TypeError):
            ctr.inc(1)

        g = m.create_metric("temperature_celsius", ["sensor"], "gauge")
        with pytest.raises(TypeError):
            g.set(42)

        h = m.create_metric("payload_bytes", ["endpoint"], "histogram")
        with pytest.raises(TypeError):
            h.observe(10)

    @pytest.mark.asyncio
    async def test_undeclared_metric_operation_is_rejected_without_registration(
        self,
    ):
        """
        Metrics not listed in METRIC_LABEL_NAMES are rejected before framework registration.
        """
        metric_name = "undeclared_metric"
        assert metric_name not in METRIC_LABEL_NAMES

        m = InMemoryMetrics()
        await m._mn_inc(metric_name, labels={"route": "/v1"})

        assert metric_name not in m.snapshot_counters()

    def test_counter_multiple_label_sets(self):
        """
        Multiple distinct label sets are tracked independently.
        """
        m = InMemoryMetrics()
        ctr = m.create_metric("jobs_total", ["queue", "status"], "counter")
        ctr.labels(queue="alpha", status="ok").inc(5)
        ctr.labels(queue="alpha", status="fail").inc(2)
        ctr.labels(queue="beta", status="ok").inc()

        assert m.snapshot_counter_value(
            "jobs_total",
            {"queue": "alpha", "status": "ok"},
        ) == 5.0
        assert m.snapshot_counter_value(
            "jobs_total",
            {"queue": "alpha", "status": "fail"},
        ) == 2.0
        assert m.snapshot_counter_value(
            "jobs_total",
            {"queue": "beta", "status": "ok"},
        ) == 1.0

    @pytest.mark.asyncio
    async def test_async_metric_operations_update_all_metric_types(self):
        """
        Drive async operations for declared framework metrics and verify snapshots.
        """
        m = InMemoryMetrics()

        # Counters
        await m._mn_inc(PIPELINE_EVENT_PRODUCED_TOTAL, amount=2, labels={LABEL_PIPELINE: "alpha"})
        await m._mn_inc(PIPELINE_EVENT_PRODUCED_TOTAL, amount=1, labels={LABEL_PIPELINE: "alpha"})
        await m._mn_inc(PIPELINE_EVENT_PRODUCED_TOTAL, amount=5, labels={LABEL_PIPELINE: "beta"})

        # Gauges (overwrite behavior)
        await m._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            11.0,
            labels={LABEL_MINION: "", LABEL_ORCHESTRATION_ID: ""},
        )
        await m._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            7.5,
            labels={LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
        )
        await m._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            9.0,
            labels={LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
        )

        # Histograms (aggregate)
        histogram_labels = {
            LABEL_STATE_STORE_TYPE: "SQLiteStateStore",
            LABEL_OPERATION: "save",
        }
        await m._mn_observe(STATE_STORE_OPERATION_DURATION_SECONDS, 0.15, labels=histogram_labels)
        await m._mn_observe(STATE_STORE_OPERATION_DURATION_SECONDS, 0.10, labels=histogram_labels)
        await m._mn_observe(STATE_STORE_OPERATION_DURATION_SECONDS, 0.25, labels=histogram_labels)

        # Assert counters
        assert m.snapshot_counter_value(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            {LABEL_PIPELINE: "alpha"},
        ) == 3.0
        assert m.snapshot_counter_value(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            {LABEL_PIPELINE: "beta"},
        ) == 5.0

        # Assert gauges
        assert m.snapshot_gauge_value(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            {LABEL_MINION: "", LABEL_ORCHESTRATION_ID: ""},
        ) == 11.0
        assert m.snapshot_gauge_value(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            {LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
        ) == 9.0

        # Assert histograms
        hsnap = m.snapshot_histograms()[STATE_STORE_OPERATION_DURATION_SECONDS]
        stats = InMemoryMetrics.find_sample(hsnap, histogram_labels)
        assert stats["count"] == 3.0
        assert stats["sum"] == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
            0.50,
            abs=1e-9,
        )

    @pytest.mark.asyncio
    async def test_async_metric_operations_require_exact_declared_label_keys(self):
        """Framework operations reject missing and extra declared label keys."""
        m = InMemoryMetrics()

        await m._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            123.0,
            labels={LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1", "unexpected": "x"},
        )
        await m._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            456.0,
            labels={LABEL_ORCHESTRATION_ID: "o1", LABEL_MINION: "m1"},
        )

        assert m.snapshot_gauge_value(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            {LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
        ) == 456.0

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ("operation", "metric_name", "labels"),
        [
            (
                "inc",
                PIPELINE_EVENT_PRODUCED_TOTAL,
                {LABEL_PIPELINE: "p1", "unexpected": "x"},
            ),
            (
                "set",
                MINION_WORKFLOW_INFLIGHT_GAUGE,
                {LABEL_MINION: "m1"},
            ),
            (
                "observe",
                STATE_STORE_OPERATION_DURATION_SECONDS,
                {LABEL_STATE_STORE_TYPE: "InMemoryStateStore"},
            ),
        ],
    )
    async def test_async_metric_operations_reject_label_key_mismatches(
        self,
        operation: str,
        metric_name: str,
        labels: dict[str, str],
    ):
        m = InMemoryMetrics()

        if operation == "inc":
            await m._mn_inc(metric_name, labels=labels)
            assert metric_name not in m.snapshot_counters()
        elif operation == "set":
            await m._mn_set(metric_name, 1.0, labels=labels)
            assert metric_name not in m.snapshot_gauges()
        elif operation == "observe":
            await m._mn_observe(metric_name, 1.0, labels=labels)
            assert metric_name not in m.snapshot_histograms()
        else:
            raise AssertionError(f"unhandled operation: {operation}")

    @pytest.mark.asyncio
    async def test_async_metric_operations_preserve_all_concurrent_updates(self):
        """
        Ensure counter updates are safe when awaited concurrently across tasks.
        """
        m = InMemoryMetrics()

        async def bump(n: int) -> None:
            for _ in range(n):
                await m._mn_inc(
                    PIPELINE_EVENT_PRODUCED_TOTAL,
                    amount=1,
                    labels={LABEL_PIPELINE: "p1"},
                )

        # 5 tasks * 200 increments = 1000
        tasks = [asyncio.create_task(bump(200)) for _ in range(5)]
        await asyncio.gather(*tasks)

        assert m.snapshot_counter_value(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            {LABEL_PIPELINE: "p1"},
        ) == 1000.0
