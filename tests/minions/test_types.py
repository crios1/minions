from dataclasses import is_dataclass

from minions.types import (
    ConflictingMinion,
    GruResult,
    ShutdownError,
    ShutdownResult,
    StartResult,
    StopResult,
)


def test_lifecycle_result_types_are_importable_from_public_types_module() -> None:
    public_types = (
        ConflictingMinion,
        GruResult,
        ShutdownError,
        ShutdownResult,
        StartResult,
        StopResult,
    )

    assert all(is_dataclass(public_type) for public_type in public_types)
    assert issubclass(ShutdownResult, GruResult)
    assert issubclass(StartResult, GruResult)
    assert issubclass(StopResult, GruResult)
