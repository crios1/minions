from dataclasses import dataclass


@dataclass(slots=True, kw_only=True, frozen=True)
class RecordEvent:
    seq: int = 1
