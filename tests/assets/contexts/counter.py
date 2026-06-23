from dataclasses import dataclass


@dataclass
class CounterContext:
    handled: bool = False
    seq: int | None = None
    value: int | None = None
    value_a: int | None = None
    value_b: int | None = None
    value_c: int | None = None
