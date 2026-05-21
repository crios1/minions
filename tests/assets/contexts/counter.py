from dataclasses import dataclass


@dataclass
class CounterContext:
    alpha: bool = False
    beta: bool = False
    config_name: str | None = None
    handled: bool = False
    seq: int | None = None
    value: int | None = None
    value_a: int | None = None
    value_b: int | None = None
    value_c: int | None = None
