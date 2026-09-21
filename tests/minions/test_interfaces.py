from dataclasses import is_dataclass
from typing import get_args, is_typeddict

from minions.interfaces import (
    CounterSample,
    GaugeSample,
    HistogramSample,
    LabelledCounter,
    LabelledGauge,
    LabelledHistogram,
    LabelledMetric,
    StoredWorkflowContext,
)


def test_extension_contract_types_are_importable_from_public_interfaces_module() -> None:
    assert all(
        getattr(protocol, "_is_protocol", False)
        for protocol in (LabelledCounter, LabelledGauge, LabelledHistogram)
    )
    assert get_args(LabelledMetric) == (
        LabelledCounter,
        LabelledGauge,
        LabelledHistogram,
    )
    assert is_dataclass(StoredWorkflowContext)


def test_metrics_snapshot_shapes_are_importable_from_public_interfaces_module() -> None:
    assert all(is_typeddict(sample) for sample in (CounterSample, GaugeSample, HistogramSample))
    assert CounterSample.__required_keys__ == {"labels", "value"}
    assert GaugeSample.__required_keys__ == {"labels", "value"}
    assert HistogramSample.__required_keys__ == {"labels", "count", "sum"}
