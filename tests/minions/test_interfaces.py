from dataclasses import is_dataclass
from typing import get_args

from minions.interfaces import (
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
