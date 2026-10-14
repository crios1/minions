from dataclasses import is_dataclass
from typing import is_typeddict

from minions.interfaces import (
    CounterSample,
    GaugeSample,
    HistogramSample,
    MetricDefinition,
    Metrics,
    SnapshotResult,
    StoredWorkflowContext,
)


def test_extension_contract_types_are_importable_from_public_interfaces_module() -> None:
    definition = MetricDefinition(
        name="request_count",
        kind="counter",
        label_names={"route"},
    )
    assert definition.name == "request_count"
    assert Metrics.__abstractmethods__ == {
        "increment_counter",
        "set_gauge",
        "observe_histogram",
        "snapshot",
    }
    assert is_dataclass(StoredWorkflowContext)


def test_metrics_snapshot_shapes_are_importable_from_public_interfaces_module() -> None:
    assert all(
        is_typeddict(sample)
        for sample in (CounterSample, GaugeSample, HistogramSample, SnapshotResult)
    )
    assert CounterSample.__required_keys__ == {"labels", "value"}
    assert GaugeSample.__required_keys__ == {"labels", "value"}
    assert HistogramSample.__required_keys__ == {"labels", "count", "sum"}
    assert SnapshotResult.__required_keys__ == {"counter", "gauge", "histogram"}
