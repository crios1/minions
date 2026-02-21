from dataclasses import dataclass

@dataclass(slots=True, kw_only=True, frozen=True)
class SimpleEvent:
    timestamp: float