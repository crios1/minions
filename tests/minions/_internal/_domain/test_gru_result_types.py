import pytest

from minions._internal._domain.gru_result_types import (
    GruResult,
    MinionStatusResult,
    ShutdownResult,
    StartResult,
    StopResult,
)


@pytest.mark.parametrize(
    "result_type",
    [GruResult, StartResult, StopResult, ShutdownResult, MinionStatusResult],
)
def test_unsuccessful_result_requires_reason(result_type: type[GruResult]) -> None:
    with pytest.raises(ValueError, match="reason.*success.*False"):
        result_type(success=False)


def test_successful_result_does_not_require_reason() -> None:
    result = StopResult(success=True)

    assert result.reason is None
