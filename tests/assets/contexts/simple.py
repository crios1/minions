from dataclasses import dataclass


@dataclass
class SimpleContext:
    price: float | None = None
    price1: float | None = None
    price2: float | None = None
    step1: str | None = None
    step2: str | None = None
