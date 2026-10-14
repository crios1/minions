import threading
from concurrent.futures import ThreadPoolExecutor

import pytest

import minions._internal._framework.metrics as metrics_module
from minions._internal._framework.logger import ERROR
from minions._internal._framework.metric_definition import Kind, MetricDefinition
from minions._internal._framework.metrics import (
    CounterSample,
    GaugeSample,
    HistogramSample,
    Metrics,
)
from minions._internal._framework.metrics_constants import (
    LABEL_MINION,
    LABEL_OPERATION,
    LABEL_ORCHESTRATION_ID,
    LABEL_PIPELINE,
    LABEL_STATE_STORE_TYPE,
    LABEL_STATUS,
    MINION_WORKFLOW_DURATION_SECONDS,
    MINION_WORKFLOW_INFLIGHT_GAUGE,
    MINION_WORKFLOW_STARTED_TOTAL,
    PIPELINE_EVENT_PRODUCED_TOTAL,
    STATE_STORE_OPERATION_DURATION_SECONDS,
)
from minions._internal._framework.metrics_prometheus import PrometheusMetrics
from tests.assets.crash.support.metrics.boom_metrics import (
    AssetMetrics as BoomMetrics,
)
from tests.assets.support.logger_inmemory import InMemoryLogger
from tests.assets.support.metrics_inmemory import InMemoryMetrics


# NoOpMetrics is excluded from the contract because it does nothing.
@pytest.fixture(
    params=[
        pytest.param(InMemoryMetrics, id="in_memory"),
        pytest.param(PrometheusMetrics, id="prometheus"),
    ],
)
def stateful_metrics_implementation(
    request: pytest.FixtureRequest,
    logger: InMemoryLogger,
) -> Metrics:
    if request.param is PrometheusMetrics:
        return request.param(logger=logger, port=0)
    return request.param(logger=logger)


class TestStatefulMetricsImplementations:
    @pytest.mark.asyncio
    async def test_metric_updates_are_reflected_in_snapshots_before_and_after_startup(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        assert metrics.snapshot() == {
            "counter": {},
            "gauge": {},
            "histogram": {},
        }

        labels = {"route": "/orders"}
        counter_definition = MetricDefinition(
            name="request_count",
            kind="counter",
            label_names={"route"},
        )
        gauge_definition = MetricDefinition(
            name="active_requests",
            kind="gauge",
            label_names={"route"},
        )
        histogram_definition = MetricDefinition(
            name="request_duration_seconds",
            kind="histogram",
            label_names={"route"},
            histogram_buckets=(1.0, 2.0),
        )

        metrics.increment_counter(counter_definition, labels=labels, amount=5.0)
        metrics.set_gauge(gauge_definition, labels=labels, value=4.0)
        metrics.observe_histogram(histogram_definition, labels=labels, value=1.0)

        expected_snapshot = {
            "counter": {
                "request_count": [{"labels": labels, "value": 5.0}],
            },
            "gauge": {
                "active_requests": [{"labels": labels, "value": 4.0}],
            },
            "histogram": {
                "request_duration_seconds": [
                    {"labels": labels, "count": 1.0, "sum": 1.0},
                ],
            },
        }
        assert metrics.snapshot() == expected_snapshot, type(metrics).__name__

        try:
            await metrics.startup()
            assert metrics.snapshot() == expected_snapshot, type(metrics).__name__
        finally:
            await metrics.shutdown()

    @pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
    def test_metric_updates_preserve_empty_and_nonempty_label_values(
        self,
        kind: Kind,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        metric_name = "metric"
        label_names = {"label_1", "label_2"}
        labels = {"label_1": "", "label_2": "non-empty"}

        if kind == "counter":
            metrics.increment_counter(
                MetricDefinition(
                    name=metric_name,
                    kind="counter",
                    label_names=label_names,
                ),
                labels=labels,
            )
            expected_sample = {"labels": labels, "value": 1.0}
        elif kind == "gauge":
            metrics.set_gauge(
                MetricDefinition(
                    name=metric_name,
                    kind="gauge",
                    label_names=label_names,
                ),
                labels=labels,
                value=2.0,
            )
            expected_sample = {"labels": labels, "value": 2.0}
        else:
            metrics.observe_histogram(
                MetricDefinition(
                    name=metric_name,
                    kind="histogram",
                    label_names=label_names,
                ),
                labels=labels,
                value=3.0,
            )
            expected_sample = {"labels": labels, "count": 1.0, "sum": 3.0}

        assert metrics.snapshot()[kind][metric_name] == [expected_sample], type(metrics).__name__

    @pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
    def test_metric_updates_reuse_one_series_per_distinct_label_set(
        self,
        kind: Kind,
        stateful_metrics_implementation: Metrics,
    ):
        """Assert repeated updates produce one snapshot sample per distinct label set.

        Exercise the empty label set and partially overlapping labeled sets for each kind.
        """
        metrics = stateful_metrics_implementation
        unlabeled_metric_name = "metric_without_labels"
        labeled_metric_name = "metric_with_labels"
        label_names = {"label_1", "label_2"}
        labels_a = {"label_1": "1", "label_2": "1"}
        labels_b = {"label_1": "1", "label_2": "2"}
        labels_c = {"label_1": "2", "label_2": "1"}

        if kind == "counter":
            definition = MetricDefinition(
                name=unlabeled_metric_name,
                kind="counter",
                label_names=set(),
            )
            metrics.increment_counter(definition, labels={}, amount=1.0)
            metrics.increment_counter(definition, labels={}, amount=1.0)

            definition = MetricDefinition(
                name=labeled_metric_name,
                kind="counter",
                label_names=label_names,
            )
            for labels, amount in (
                (labels_a, 1.0),
                (labels_b, 2.0),
                (labels_c, 3.0),
                (labels_c, 4.0),
            ):
                metrics.increment_counter(definition, labels=labels, amount=amount)

            snapshot = metrics.snapshot()

            assert snapshot["counter"][unlabeled_metric_name] == [
                {"labels": {}, "value": 2.0}
            ], type(metrics).__name__

            samples = snapshot["counter"][labeled_metric_name]
            assert len(samples) == 3, type(metrics).__name__
            assert {
                frozenset(sample["labels"].items()): sample["value"]
                for sample in samples
            } == {
                frozenset(labels_a.items()): 1.0,
                frozenset(labels_b.items()): 2.0,
                frozenset(labels_c.items()): 7.0,
            }, type(metrics).__name__

            assert snapshot["gauge"] == {}, type(metrics).__name__
            assert snapshot["histogram"] == {}, type(metrics).__name__

        elif kind == "gauge":
            definition = MetricDefinition(
                name=unlabeled_metric_name,
                kind="gauge",
                label_names=set(),
            )
            metrics.set_gauge(definition, labels={}, value=1.0)
            metrics.set_gauge(definition, labels={}, value=2.0)

            definition = MetricDefinition(
                name=labeled_metric_name,
                kind="gauge",
                label_names=label_names,
            )
            for labels, value in (
                (labels_a, 1.0),
                (labels_b, 2.0),
                (labels_c, 3.0),
                (labels_c, 4.0),
            ):
                metrics.set_gauge(definition, labels=labels, value=value)

            snapshot = metrics.snapshot()

            assert snapshot["gauge"][unlabeled_metric_name] == [
                {"labels": {}, "value": 2.0}
            ], type(metrics).__name__

            samples = snapshot["gauge"][labeled_metric_name]
            assert len(samples) == 3, type(metrics).__name__
            assert {
                frozenset(sample["labels"].items()): sample["value"]
                for sample in samples
            } == {
                frozenset(labels_a.items()): 1.0,
                frozenset(labels_b.items()): 2.0,
                frozenset(labels_c.items()): 4.0,
            }, type(metrics).__name__

            assert snapshot["counter"] == {}, type(metrics).__name__
            assert snapshot["histogram"] == {}, type(metrics).__name__

        elif kind == "histogram":
            definition = MetricDefinition(
                name=unlabeled_metric_name,
                kind="histogram",
                label_names=set(),
            )
            metrics.observe_histogram(definition, labels={}, value=1.0)
            metrics.observe_histogram(definition, labels={}, value=2.0)

            definition = MetricDefinition(
                name=labeled_metric_name,
                kind="histogram",
                label_names=label_names,
            )
            for labels, value in (
                (labels_a, 1.0),
                (labels_b, 2.0),
                (labels_c, 3.0),
                (labels_c, 4.0),
            ):
                metrics.observe_histogram(definition, labels=labels, value=value)

            snapshot = metrics.snapshot()

            assert snapshot["histogram"][unlabeled_metric_name] == [
                {"labels": {}, "count": 2.0, "sum": 3.0}
            ], type(metrics).__name__

            samples = snapshot["histogram"][labeled_metric_name]
            assert len(samples) == 3, type(metrics).__name__
            assert {
                frozenset(sample["labels"].items()): (sample["count"], sample["sum"])
                for sample in samples
            } == {
                frozenset(labels_a.items()): (1.0, 1.0),
                frozenset(labels_b.items()): (1.0, 2.0),
                frozenset(labels_c.items()): (2.0, 7.0),
            }, type(metrics).__name__

            assert snapshot["counter"] == {}, type(metrics).__name__
            assert snapshot["gauge"] == {}, type(metrics).__name__

        else:
            raise AssertionError(f"unexpected kind: {kind}")

    @pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
    def test_metric_updates_require_explicit_labels_argument(
        self,
        kind: Kind,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        definition = MetricDefinition(
            name=f"explicit_labels_{kind}",
            kind=kind,
            label_names=set(),
        )

        if kind == "counter":
            method = getattr(metrics, "increment_counter")
            method_kwargs = {"amount": 1.0}
        elif kind == "gauge":
            method = getattr(metrics, "set_gauge")
            method_kwargs = {"value": 1.0}
        else:
            method = getattr(metrics, "observe_histogram")
            method_kwargs = {"value": 1.0}

        with pytest.raises(TypeError, match="labels"):
            method(definition, **method_kwargs)

        method(definition, labels={}, **method_kwargs)

    @pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
    def test_snapshot_preserves_metric_names_ending_in_total(
        self,
        kind: Kind,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        metric_name = f"{kind}_total"

        expected_sample: CounterSample | GaugeSample | HistogramSample
        if kind == "counter":
            metrics.increment_counter(
                MetricDefinition(
                    name=metric_name,
                    kind="counter",
                    label_names=set(),
                ),
                labels={},
                amount=1.0,
            )
            expected_sample = {"labels": {}, "value": 1.0}
        elif kind == "gauge":
            metrics.set_gauge(
                MetricDefinition(
                    name=metric_name,
                    kind="gauge",
                    label_names=set(),
                ),
                labels={},
                value=2.0,
            )
            expected_sample = {"labels": {}, "value": 2.0}
        else:
            metrics.observe_histogram(
                MetricDefinition(
                    name=metric_name,
                    kind="histogram",
                    label_names=set(),
                ),
                labels={},
                value=3.0,
            )
            expected_sample = {"labels": {}, "count": 1.0, "sum": 3.0}

        assert metrics.snapshot()[kind][metric_name] == [expected_sample], type(metrics).__name__

    def test_rejects_different_kind_for_registered_metric_name(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        metrics.set_gauge(
            MetricDefinition(
                name="shared_metric",
                kind="gauge",
                label_names={"route", "status"},
            ),
            labels={"route": "/orders", "status": "200"},
            value=1.0,
        )

        with pytest.raises(
            ValueError,
            match=(
                r"Metric name 'shared_metric' is already associated with "
                r"definition\(kind='gauge'.*requested definition\(kind='counter'"
            ),
        ):
            metrics.increment_counter(
                MetricDefinition(
                    name="shared_metric",
                    kind="counter",
                    label_names={"route", "status"},
                ),
                labels={"route": "/orders", "status": "200"},
                amount=1.0,
            )

        assert metrics.snapshot()["gauge"]["shared_metric"] == [
            {"labels": {"route": "/orders", "status": "200"}, "value": 1.0}
        ], type(metrics).__name__

    def test_rejects_different_label_names_for_registered_metric_name(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        metrics.set_gauge(
            MetricDefinition(
                name="shared_metric",
                kind="gauge",
                label_names={"route", "status"},
            ),
            labels={"route": "/orders", "status": "200"},
            value=1.0,
        )

        with pytest.raises(
            ValueError,
            match=(
                r"Metric name 'shared_metric' is already associated with "
                r"definition\(kind='gauge', label_names=\['route', 'status'\].*"
                r"requested definition\(kind='gauge', label_names=\['route'\]"
            ),
        ):
            metrics.set_gauge(
                MetricDefinition(
                    name="shared_metric",
                    kind="gauge",
                    label_names={"route"},
                ),
                labels={"route": "/orders"},
                value=2.0,
            )

        assert metrics.snapshot()["gauge"]["shared_metric"] == [
            {"labels": {"route": "/orders", "status": "200"}, "value": 1.0}
        ], type(metrics).__name__

    def test_rejects_different_histogram_buckets_for_registered_metric_name(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        metrics.observe_histogram(
            MetricDefinition(
                name="request_duration_seconds",
                kind="histogram",
                label_names=set(),
                histogram_buckets=(1.0, 2.0),
            ),
            labels={},
            value=0.75,
        )

        with pytest.raises(
            ValueError,
            match=(
                r"Metric name 'request_duration_seconds' is already associated with "
                r"definition\(kind='histogram'.*histogram_buckets=\(1\.0, 2\.0\).*"
                r"requested definition\(kind='histogram'.*histogram_buckets=\(1\.0, 3\.0\)"
            ),
        ):
            metrics.observe_histogram(
                MetricDefinition(
                    name="request_duration_seconds",
                    kind="histogram",
                    label_names=set(),
                    histogram_buckets=(1.0, 3.0),
                ),
                labels={},
                value=0.75,
            )

        assert (
            {
                "labels": {},
                "count": 1.0,
                "sum": 0.75,
            }
            in metrics.snapshot()["histogram"]["request_duration_seconds"]
        ), type(metrics).__name__

    def test_concurrent_counter_increments_are_all_recorded(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        worker_count = 8
        update_start_barrier = threading.Barrier(
            worker_count + 1  # workers and this test thread
        )
        definition = MetricDefinition(
            name="concurrent_updates",
            kind="counter",
            label_names={"route", "status"},
        )
        labels = {"route": "/orders", "status": "200"}

        def record_metric(_index: int) -> None:
            update_start_barrier.wait(timeout=5)
            for _ in range(4):
                metrics.increment_counter(definition, labels=labels, amount=1.0)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(record_metric, index) for index in range(worker_count)
            ]
            update_start_barrier.wait(timeout=5)
            for future in futures:
                future.result(timeout=5)

        assert metrics.snapshot()["counter"]["concurrent_updates"] == [
            {"labels": {"route": "/orders", "status": "200"}, "value": 32.0}
        ], type(metrics).__name__

    def test_concurrent_gauge_sets_for_distinct_label_series_are_all_recorded(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        worker_count = 8
        write_start_barrier = threading.Barrier(
            worker_count + 1  # workers and this test thread
        )
        definition = MetricDefinition(
            name="concurrent_gauge_values",
            kind="gauge",
            label_names={"worker"},
        )

        def set_worker_gauge(worker_index: int) -> None:
            write_start_barrier.wait(timeout=5)
            metrics.set_gauge(
                definition,
                labels={"worker": f"worker-{worker_index}"},
                value=float(worker_index + 1),
            )

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(set_worker_gauge, worker_index)
                for worker_index in range(worker_count)
            ]
            write_start_barrier.wait(timeout=5)
            for future in futures:
                future.result(timeout=5)

        samples_by_worker = {
            sample["labels"]["worker"]: sample["value"]
            for sample in metrics.snapshot()["gauge"]["concurrent_gauge_values"]
        }
        assert samples_by_worker == {
            f"worker-{worker_index}": float(worker_index + 1)
            for worker_index in range(worker_count)
        }, type(metrics).__name__

    def test_concurrent_histogram_observations_are_all_recorded(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        worker_count = 8
        observation_start_barrier = threading.Barrier(
            worker_count + 1  # workers and this test thread
        )
        definition = MetricDefinition(
            name="concurrent_observations",
            kind="histogram",
            label_names={"route", "status"},
            histogram_buckets=(1.0, 2.0),
        )
        labels = {"route": "/orders", "status": "200"}

        def record_observations(_index: int) -> None:
            observation_start_barrier.wait(timeout=5)
            # Avoid bucket boundaries; this test targets concurrent recording.
            for _ in range(4):
                metrics.observe_histogram(definition, labels=labels, value=0.5)

        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            futures = [
                executor.submit(record_observations, index)
                for index in range(worker_count)
            ]
            observation_start_barrier.wait(timeout=5)
            for future in futures:
                future.result(timeout=5)

        assert metrics.snapshot()["histogram"]["concurrent_observations"] == [
            {"labels": labels, "count": 32.0, "sum": 16.0}
        ], type(metrics).__name__


class TestMetricsFrameworkWrappers:
    @pytest.mark.asyncio
    async def test_metric_updates_reuse_one_series_per_distinct_label_set(
        self,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation

        await metrics._mn_inc(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            amount=1,
            labels={LABEL_PIPELINE: "alpha"},
        )
        await metrics._mn_inc(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            amount=2,
            labels={LABEL_PIPELINE: "alpha"},
        )
        await metrics._mn_inc(
            PIPELINE_EVENT_PRODUCED_TOTAL,
            amount=5,
            labels={LABEL_PIPELINE: "beta"},
        )

        await metrics._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            10.0,
            labels={LABEL_MINION: "", LABEL_ORCHESTRATION_ID: ""},
        )
        await metrics._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            5.0,
            labels={LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
        )
        await metrics._mn_set(
            MINION_WORKFLOW_INFLIGHT_GAUGE,
            7.0,
            labels={LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
        )

        save_labels = {
            LABEL_STATE_STORE_TYPE: "SQLiteStateStore",
            LABEL_OPERATION: "save",
        }
        await metrics._mn_observe(
            STATE_STORE_OPERATION_DURATION_SECONDS,
            0.10,
            labels=save_labels,
        )
        await metrics._mn_observe(
            STATE_STORE_OPERATION_DURATION_SECONDS,
            0.20,
            labels=save_labels,
        )

        load_labels = {
            LABEL_STATE_STORE_TYPE: "SQLiteStateStore",
            LABEL_OPERATION: "load",
        }
        await metrics._mn_observe(
            STATE_STORE_OPERATION_DURATION_SECONDS,
            0.30,
            labels=load_labels,
        )
        await metrics._mn_observe(
            STATE_STORE_OPERATION_DURATION_SECONDS,
            0.40,
            labels=load_labels,
        )

        snapshot = metrics.snapshot()

        counter_samples = snapshot["counter"][PIPELINE_EVENT_PRODUCED_TOTAL]
        assert len(counter_samples) == 2, type(metrics).__name__
        counter_values = {
            sample["labels"][LABEL_PIPELINE]: sample["value"] for sample in counter_samples
        }
        assert counter_values == {"alpha": 3.0, "beta": 5.0}, type(metrics).__name__

        gauge_samples = snapshot["gauge"][MINION_WORKFLOW_INFLIGHT_GAUGE]
        assert len(gauge_samples) == 2, type(metrics).__name__
        gauge_values = {
            frozenset(sample["labels"].items()): sample["value"] for sample in gauge_samples
        }
        assert gauge_values == {
            frozenset({LABEL_MINION: "", LABEL_ORCHESTRATION_ID: ""}.items()): 10.0,
            frozenset({LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"}.items()): 7.0,
        }, type(metrics).__name__

        histogram_samples = snapshot["histogram"][STATE_STORE_OPERATION_DURATION_SECONDS]
        assert len(histogram_samples) == 2, type(metrics).__name__
        histogram_values = {
            frozenset(sample["labels"].items()): (sample["count"], sample["sum"])
            for sample in histogram_samples
        }
        assert histogram_values[frozenset(save_labels.items())][0] == 2.0
        assert histogram_values[frozenset(save_labels.items())][1] == pytest.approx(0.30)  # pyright: ignore[reportUnknownMemberType]
        assert histogram_values[frozenset(load_labels.items())][0] == 2.0
        assert histogram_values[frozenset(load_labels.items())][1] == pytest.approx(0.70)  # pyright: ignore[reportUnknownMemberType]

    @pytest.mark.parametrize(
        ("operation", "kind"),
        [
            ("inc", "counter"),
            ("set", "gauge"),
            ("observe", "histogram"),
        ],
        ids=["counter", "gauge", "histogram"],
    )
    @pytest.mark.asyncio
    async def test_metric_updates_accept_omitted_labels_for_declared_zero_label_metrics(
        self,
        operation: str,
        kind: Kind,
        logger: InMemoryLogger,
        stateful_metrics_implementation: Metrics,
        monkeypatch: pytest.MonkeyPatch,
    ):
        metrics = stateful_metrics_implementation
        metric_name = f"zero_label_{kind}_metric"
        metric_definitions = dict(metrics_module.METRIC_DEFINITIONS)
        metric_definitions[metric_name] = MetricDefinition(
            name=metric_name,
            kind=kind,
            label_names=set(),
        )
        monkeypatch.setattr(metrics_module, "METRIC_DEFINITIONS", metric_definitions)

        expected_sample: CounterSample | GaugeSample | HistogramSample
        if operation == "inc":
            await metrics._mn_inc(metric_name)
            expected_sample = {"labels": {}, "value": 1.0}
        elif operation == "set":
            await metrics._mn_set(metric_name, value=2.0)
            expected_sample = {"labels": {}, "value": 2.0}
        elif operation == "observe":
            await metrics._mn_observe(metric_name, value=3.0)
            expected_sample = {"labels": {}, "count": 1.0, "sum": 3.0}
        else:
            raise AssertionError(f"unhandled operation: {operation}")

        assert metrics.snapshot()[kind][metric_name] == [expected_sample], type(metrics).__name__
        assert logger.logs == []

    @pytest.mark.parametrize("kind", ["counter", "gauge", "histogram"])
    @pytest.mark.asyncio
    async def test_metric_updates_omitting_required_labels_are_logged_and_not_recorded(
        self,
        kind: Kind,
        logger: InMemoryLogger,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        if kind == "counter":
            metric_name = PIPELINE_EVENT_PRODUCED_TOTAL
            assert await metrics._mn_inc(metric_name) is None
        elif kind == "gauge":
            metric_name = MINION_WORKFLOW_INFLIGHT_GAUGE
            assert await metrics._mn_set(metric_name, value=10.5) is None
        else:
            metric_name = STATE_STORE_OPERATION_DURATION_SECONDS
            assert await metrics._mn_observe(metric_name, value=0.5) is None

        assert len(logger.logs) == 1
        assert logger.logs[0].kwargs["error_type"] == "ValueError"
        assert "label keys do not match declared schema" in logger.logs[0].kwargs["error_message"]
        assert metrics.snapshot()[kind] == {}, type(metrics).__name__

    @pytest.mark.parametrize(
        "operation",
        ["inc", "set", "observe"],
    )
    @pytest.mark.asyncio
    async def test_undeclared_metric_updates_are_logged_and_not_recorded(
        self,
        operation: str,
        logger: InMemoryLogger,
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation
        metric_name = f"undeclared_{operation}_metric"

        if operation == "inc":
            await metrics._mn_inc(metric_name, amount=1, labels={"route": "/v1"})
        elif operation == "set":
            await metrics._mn_set(metric_name, 2, labels={"route": "/v1"})
        else:
            await metrics._mn_observe(metric_name, 3, labels={"route": "/v1"})

        assert metrics.snapshot() == {
            "counter": {},
            "gauge": {},
            "histogram": {},
        }, type(metrics).__name__
        assert len(logger.logs) == 1
        assert all(log.level == ERROR for log in logger.logs)
        assert all(
            log.msg.endswith(f"._mn_{operation} failed") for log in logger.logs
        )
        assert all(log.kwargs["error_type"] == "ValueError" for log in logger.logs)
        assert all(
            log.kwargs["error_message"]
            == f"Undeclared metric '{metric_name}': no declared definition"
            for log in logger.logs
        )

    @pytest.mark.parametrize(
        ("operation", "kind", "metric_name", "invalid_labels", "valid_labels"),
        [
            (
                "inc",
                "counter",
                PIPELINE_EVENT_PRODUCED_TOTAL,
                {LABEL_PIPELINE: "p1", "unexpected": "x"},
                {LABEL_PIPELINE: "p1"},
            ),
            (
                "set",
                "gauge",
                MINION_WORKFLOW_INFLIGHT_GAUGE,
                {LABEL_MINION: "m1"},
                {LABEL_MINION: "m1", LABEL_ORCHESTRATION_ID: "o1"},
            ),
            (
                "observe",
                "histogram",
                STATE_STORE_OPERATION_DURATION_SECONDS,
                {LABEL_STATE_STORE_TYPE: "InMemoryStateStore"},
                {
                    LABEL_STATE_STORE_TYPE: "InMemoryStateStore",
                    LABEL_OPERATION: "save",
                },
            ),
        ],
    )
    @pytest.mark.asyncio
    async def test_label_mismatch_does_not_prevent_later_valid_update_for_same_metric(
        self,
        operation: str,
        kind: Kind,
        metric_name: str,
        invalid_labels: dict[str, str],
        valid_labels: dict[str, str],
        stateful_metrics_implementation: Metrics,
    ):
        metrics = stateful_metrics_implementation

        if operation == "inc":
            await metrics._mn_inc(metric_name, labels=invalid_labels)
            assert metric_name not in metrics.snapshot()[kind], type(metrics).__name__

            await metrics._mn_inc(metric_name, labels=valid_labels)
            assert metrics.snapshot()[kind][metric_name] == [
                {"labels": valid_labels, "value": 1.0}
            ], type(metrics).__name__
        elif operation == "set":
            await metrics._mn_set(metric_name, 123.0, labels=invalid_labels)
            assert metric_name not in metrics.snapshot()[kind], type(metrics).__name__

            await metrics._mn_set(metric_name, 456.0, labels=valid_labels)
            assert metrics.snapshot()[kind][metric_name] == [
                {"labels": valid_labels, "value": 456.0}
            ], type(metrics).__name__
        elif operation == "observe":
            await metrics._mn_observe(metric_name, 0.15, labels=invalid_labels)
            assert metric_name not in metrics.snapshot()[kind], type(metrics).__name__

            await metrics._mn_observe(metric_name, 0.25, labels=valid_labels)
            assert metrics.snapshot()[kind][metric_name] == [
                {"labels": valid_labels, "count": 1.0, "sum": 0.25}
            ], type(metrics).__name__
        else:
            raise AssertionError(f"unhandled operation: {operation}")

    @pytest.mark.parametrize("metric_operation", ["inc", "set", "observe"])
    @pytest.mark.asyncio
    async def test_metric_recording_failures_are_contained(
        self,
        metric_operation: str,
        logger: InMemoryLogger,
    ):
        metrics = BoomMetrics(logger=logger)

        if metric_operation == "inc":
            assert (
                await metrics._mn_inc(
                    MINION_WORKFLOW_STARTED_TOTAL,
                    labels={
                        LABEL_MINION: "test-minion",
                        LABEL_ORCHESTRATION_ID: "test-orchestration",
                    },
                )
                is None
            )
            assert logger.has_log("AssetMetrics._mn_inc failed")
        elif metric_operation == "set":
            assert (
                await metrics._mn_set(
                    MINION_WORKFLOW_INFLIGHT_GAUGE,
                    1.0,
                    labels={
                        LABEL_MINION: "test-minion",
                        LABEL_ORCHESTRATION_ID: "test-orchestration",
                    },
                )
                is None
            )
            assert logger.has_log("AssetMetrics._mn_set failed")
        elif metric_operation == "observe":
            assert (
                await metrics._mn_observe(
                    MINION_WORKFLOW_DURATION_SECONDS,
                    1.0,
                    labels={
                        LABEL_MINION: "test-minion",
                        LABEL_ORCHESTRATION_ID: "test-orchestration",
                        LABEL_STATUS: "success",
                    },
                )
                is None
            )
            assert logger.has_log("AssetMetrics._mn_observe failed")
        else:
            raise AssertionError(f"unhandled metric operation: {metric_operation}")

        assert any(log.kwargs.get("error_type") == "BoomError" for log in logger.logs)

    @pytest.mark.asyncio
    async def test_snapshot_failures_are_contained(
        self,
        logger: InMemoryLogger,
    ):
        metrics = BoomMetrics(logger=logger)

        assert await metrics._mn_snapshot() is None
        assert logger.has_log("AssetMetrics.snapshot failed")
