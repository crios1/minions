from dataclasses import dataclass


@dataclass(frozen=True)
class CounterEvent:
    seq: int
