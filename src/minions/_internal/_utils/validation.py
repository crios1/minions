import math


def _ensure_finite_number(value: object, *, label: str) -> float:
    if not label.strip():
        raise ValueError("label must not be blank")
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{label} must be a finite number")
    try:
        number = float(value)
    except OverflowError:
        raise ValueError(f"{label} must be within the finite float range") from None
    except ValueError:
        raise ValueError(f"{label} must be a finite number") from None
    if not math.isfinite(number):
        raise ValueError(f"{label} must be a finite number")
    return number


def ensure_positive_number(value: object, *, label: str) -> float:
    number = _ensure_finite_number(value, label=label)
    if number <= 0:
        raise ValueError(f"{label} must be a positive number")
    return number


def ensure_nonnegative_number(value: object, *, label: str) -> float:
    number = _ensure_finite_number(value, label=label)
    if number < 0:
        raise ValueError(f"{label} must be a non-negative number")
    return number


def ensure_number_at_least(
    value: object,
    threshold: float,
    *,
    label: str,
) -> float:
    number = _ensure_finite_number(value, label=label)
    finite_threshold = _ensure_finite_number(threshold, label="threshold")
    if number < finite_threshold:
        raise ValueError(
            f"{label} must be a number greater than or equal to {finite_threshold:g}"
        )
    return number


def ensure_number_in_closed_range(
    value: object,
    *,
    minimum: float,
    maximum: float,
    label: str,
) -> float:
    number = _ensure_finite_number(value, label=label)
    finite_minimum = _ensure_finite_number(minimum, label="minimum")
    finite_maximum = _ensure_finite_number(maximum, label="maximum")
    if finite_minimum > finite_maximum:
        raise ValueError("minimum must be less than or equal to maximum")
    if number < finite_minimum or number > finite_maximum:
        raise ValueError(
            f"{label} must be a number between {finite_minimum:g} and "
            f"{finite_maximum:g}, inclusive"
        )
    return number
