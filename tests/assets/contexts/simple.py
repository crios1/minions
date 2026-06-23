from dataclasses import dataclass


@dataclass
class SimpleContext:
    value: int | None = None
    value_a: int | None = None
    value_b: int | None = None
    step1: str | None = None
    step2: str | None = None
