import asyncio

import pytest

from minions._internal._framework.metrics_constants import METRIC_LABEL_NAMES
from tests.assets.support.metrics_inmemory import InMemoryMetrics


class TestInMemoryMetrics:
    def test_counter_healthy_with_label_ordering(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Counter increments aggregate correctly; labels follow METRIC_LABEL_NAMES order.
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "my_counter", ["minion", "pipeline"])

        m = InMemoryMetrics()
        # Get backend metric and exercise via LabelledMetric API
        counter = m._mn_get_metric_unsafe("counter", "my_counter")
        counter.labels(minion="m1", pipeline="p1").inc(2)
        counter.labels(minion="m1", pipeline="p1").inc()  # +1

        samples = m.snapshot_counters()["my_counter"]
        assert len(samples) == 1
        sample = samples[0]
        assert sample["labels"] == {"minion": "m1", "pipeline": "p1"}
        assert list(sample["labels"].keys()) == ["minion", "pipeline"]
        assert sample["value"] == 3.0

    def test_gauge_set_with_missing_label_defaults(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Gauge .set() overwrites, and missing expected labels default to "".
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "cpu_gauge", ["region"])

        m = InMemoryMetrics()
        gauge = m._mn_get_metric_unsafe("gauge", "cpu_gauge")
        gauge.labels().set(10.5)  # region defaults to ""
        gauge.labels(region="us-east").set(7.0)
        gauge.labels(region="us-east").set(8.0)  # overwrite

        samples = m.snapshot_gauges()["cpu_gauge"]
        assert InMemoryMetrics.find_sample(samples, {"region": ""})["value"] == 10.5
        assert InMemoryMetrics.find_sample(samples, {"region": "us-east"})["value"] == 8.0

    def test_histogram_observe_aggregates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Histogram aggregates count/sum/min/max for a label set.
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "latency_seconds", ["route"])

        m = InMemoryMetrics()
        h = m._mn_get_metric_unsafe("histogram", "latency_seconds")
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

    def test_counter_value_total_across_label_sets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(METRIC_LABEL_NAMES, "requests_total", ["route"])

        metrics = InMemoryMetrics()
        counter = metrics._mn_get_metric_unsafe("counter", "requests_total")
        counter.labels(route="/a").inc(2)
        counter.labels(route="/b").inc(3)

        assert metrics.counter_value_total("requests_total") == 5

    def test_gauge_value_total_across_label_sets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(METRIC_LABEL_NAMES, "connections", ["host"])

        metrics = InMemoryMetrics()
        gauge = metrics._mn_get_metric_unsafe("gauge", "connections")
        gauge.labels(host="a").set(4)
        gauge.labels(host="b").set(5)

        assert metrics.gauge_value_total("connections") == 9

    def test_histogram_count_total_across_label_sets(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setitem(METRIC_LABEL_NAMES, "latency_seconds", ["route"])

        metrics = InMemoryMetrics()
        histogram = metrics._mn_get_metric_unsafe("histogram", "latency_seconds")
        histogram.labels(route="/a").observe(0.1)
        histogram.labels(route="/b").observe(0.2)
        histogram.labels(route="/b").observe(0.3)

        assert metrics.histogram_count_total("latency_seconds") == 3

    def test_histogram_sum_total_across_label_sets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setitem(METRIC_LABEL_NAMES, "latency_seconds", ["route"])

        metrics = InMemoryMetrics()
        histogram = metrics._mn_get_metric_unsafe("histogram", "latency_seconds")
        histogram.labels(route="/a").observe(0.1)
        histogram.labels(route="/b").observe(0.2)
        histogram.labels(route="/b").observe(0.3)

        assert metrics.histogram_sum_total("latency_seconds") == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
            0.6,
            abs=1e-9,
        )

    def test_unbound_metric_methods_raise(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Calling .inc/.set/.observe on the unbound metric (without .labels()) should raise TypeError.
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "events_total", ["minion"])
        monkeypatch.setitem(METRIC_LABEL_NAMES, "temperature_celsius", ["sensor"])
        monkeypatch.setitem(METRIC_LABEL_NAMES, "payload_bytes", ["endpoint"])
        m = InMemoryMetrics()

        ctr = m._mn_get_metric_unsafe("counter", "events_total")
        with pytest.raises(TypeError):
            ctr.inc(1)

        g = m._mn_get_metric_unsafe("gauge", "temperature_celsius")
        with pytest.raises(TypeError):
            g.set(42)

        h = m._mn_get_metric_unsafe("histogram", "payload_bytes")
        with pytest.raises(TypeError):
            h.observe(10)

    @pytest.mark.asyncio
    async def test_unknown_metric_label_sorting(self):
        """
        For metrics not listed in METRIC_LABEL_NAMES, labels are accepted and sorted by key.
        """
        # Ensure our metric is not pre-declared in METRIC_LABEL_NAMES
        METRIC_NAME = "unlisted_metric"
        if METRIC_NAME in METRIC_LABEL_NAMES:
            del METRIC_LABEL_NAMES[METRIC_NAME]

        m = InMemoryMetrics()
        # Provide labels in reverse order; snapshot keys should be sorted ('a','b')
        await m._mn_inc(METRIC_NAME, labels={"b": "2", "a": "1"})

        samples = m.snapshot_counters()[METRIC_NAME]
        assert len(samples) == 1
        sample = samples[0]
        assert sample["labels"] == {"a": "1", "b": "2"}
        assert list(sample["labels"].keys()) == ["a", "b"]
        assert sample["value"] == 1.0

    def test_counter_multiple_label_sets(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Multiple distinct label sets are tracked independently.
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "jobs_total", ["queue", "status"])

        m = InMemoryMetrics()
        ctr = m._mn_get_metric_unsafe("counter", "jobs_total")
        ctr.labels(queue="alpha", status="ok").inc(5)
        ctr.labels(queue="alpha", status="fail").inc(2)
        ctr.labels(queue="beta", status="ok").inc()

        samples = m.snapshot_counters()["jobs_total"]
        assert (
            InMemoryMetrics.find_sample(samples, {"queue": "alpha", "status": "ok"})["value"] == 5.0
        )
        assert (
            InMemoryMetrics.find_sample(samples, {"queue": "alpha", "status": "fail"})["value"] == 2.0  # noqa: E501
        )
        assert (
            InMemoryMetrics.find_sample(samples, {"queue": "beta", "status": "ok"})["value"] == 1.0
        )

    @pytest.mark.asyncio
    async def test_metrics_async_healthy(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Drive the async facade and verify snapshots reflect the updates.
        """
        # Declare expected labels for stable ordering/defaults
        monkeypatch.setitem(METRIC_LABEL_NAMES, "jobs_total", ["queue", "status"])
        monkeypatch.setitem(METRIC_LABEL_NAMES, "cpu_used_percent", ["region"])
        monkeypatch.setitem(METRIC_LABEL_NAMES, "op_latency_seconds", ["route"])

        m = InMemoryMetrics()

        # Counters
        await m._mn_inc("jobs_total", amount=2, labels={"queue": "alpha", "status": "ok"})
        await m._mn_inc("jobs_total", amount=1, labels={"queue": "alpha", "status": "ok"})
        await m._mn_inc("jobs_total", amount=5, labels={"queue": "beta", "status": "fail"})

        # Gauges (overwrite behavior)
        await m._mn_set("cpu_used_percent", 11.0)  # region defaults to ""
        await m._mn_set("cpu_used_percent", 7.5, labels={"region": "us"})  # set explicit
        await m._mn_set("cpu_used_percent", 9.0, labels={"region": "us"})  # overwrite

        # Histograms (aggregate)
        await m._mn_observe("op_latency_seconds", 0.15, labels={"route": "/v1/foo"})
        await m._mn_observe("op_latency_seconds", 0.10, labels={"route": "/v1/foo"})
        await m._mn_observe("op_latency_seconds", 0.25, labels={"route": "/v1/foo"})

        # Assert counters
        csnap = m.snapshot_counters()["jobs_total"]
        assert (
            InMemoryMetrics.find_sample(csnap, {"queue": "alpha", "status": "ok"})["value"] == 3.0
        )
        assert (
            InMemoryMetrics.find_sample(csnap, {"queue": "beta", "status": "fail"})["value"] == 5.0
        )

        # Assert gauges
        gsnap = m.snapshot_gauges()["cpu_used_percent"]
        assert InMemoryMetrics.find_sample(gsnap, {"region": ""})["value"] == 11.0
        assert (
            InMemoryMetrics.find_sample(gsnap, {"region": "us"})["value"] == 9.0
        )  # last write wins

        # Assert histograms
        hsnap = m.snapshot_histograms()["op_latency_seconds"]
        stats = InMemoryMetrics.find_sample(hsnap, {"route": "/v1/foo"})
        assert stats["count"] == 3.0
        assert stats["sum"] == pytest.approx(  # pyright: ignore[reportUnknownMemberType]
            0.50,
            abs=1e-9,
        )

    @pytest.mark.asyncio
    async def test_metrics_async_label_defaults_and_ordering(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """
        Missing expected labels default to "" and ordering follows METRIC_LABEL_NAMES.
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "mem_used_bytes", ["host", "region"])
        m = InMemoryMetrics()

        # host present, region missing -> defaults to ""
        await m._mn_set("mem_used_bytes", 123.0, labels={"host": "h1"})
        # both present, order in kwargs shouldn't matter
        await m._mn_set("mem_used_bytes", 456.0, labels={"region": "us-east", "host": "h1"})

        gsnap = m.snapshot_gauges()["mem_used_bytes"]
        assert InMemoryMetrics.find_sample(gsnap, {"host": "h1", "region": ""})["value"] == 123.0
        assert (
            InMemoryMetrics.find_sample(gsnap, {"host": "h1", "region": "us-east"})["value"] == 456.0  # noqa: E501
        )

    @pytest.mark.asyncio
    async def test_metrics_async_concurrent_updates(self, monkeypatch: pytest.MonkeyPatch) -> None:
        """
        Ensure counter updates are safe when awaited concurrently across tasks.
        """
        monkeypatch.setitem(METRIC_LABEL_NAMES, "events_total", ["minion"])
        m = InMemoryMetrics()

        async def bump(n: int) -> None:
            for _ in range(n):
                await m._mn_inc("events_total", amount=1, labels={"minion": "m1"})

        # 5 tasks * 200 increments = 1000
        tasks = [asyncio.create_task(bump(200)) for _ in range(5)]
        await asyncio.gather(*tasks)

        csnap = m.snapshot_counters()["events_total"]
        assert InMemoryMetrics.find_sample(csnap, {"minion": "m1"})["value"] == 1000.0
