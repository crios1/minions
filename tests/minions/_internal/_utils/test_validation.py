import math

import pytest

from minions._internal._utils.validation import (
    ensure_nonnegative_number,
    ensure_number_at_least,
    ensure_number_in_closed_range,
    ensure_positive_number,
)


@pytest.mark.parametrize("label", ["", " "])
def test_requires_nonblank_label(label: str):
    """Exercise every validator in validation.py against blank labels."""

    with pytest.raises(ValueError, match="label must not be blank"):
        ensure_positive_number(1, label=label)

    with pytest.raises(ValueError, match="label must not be blank"):
        ensure_nonnegative_number(1, label=label)

    with pytest.raises(ValueError, match="label must not be blank"):
        ensure_number_at_least(1, 1, label=label)

    with pytest.raises(ValueError, match="label must not be blank"):
        ensure_number_in_closed_range(
            1,
            minimum=0,
            maximum=1,
            label=label,
        )


@pytest.mark.parametrize("value", [True, "1", math.nan, math.inf, -math.inf])
def test_rejects_values_that_are_not_finite_numbers(
    value: object,
):
    """Exercise every numeric validator against values outside its numeric contract."""

    with pytest.raises(ValueError, match="value must be a finite number"):
        ensure_positive_number(value, label="value")

    with pytest.raises(ValueError, match="value must be a finite number"):
        ensure_nonnegative_number(value, label="value")

    with pytest.raises(ValueError, match="value must be a finite number"):
        ensure_number_at_least(value, 1, label="value")

    with pytest.raises(ValueError, match="value must be a finite number"):
        ensure_number_in_closed_range(
            value,
            minimum=0,
            maximum=1,
            label="value",
        )


def test_rejects_integers_outside_float_range():
    """Exercise every numeric validator against integers outside float range."""
    value = 10**1000

    with pytest.raises(ValueError, match="value must be within the finite float range"):
        ensure_positive_number(value, label="value")

    with pytest.raises(ValueError, match="value must be within the finite float range"):
        ensure_nonnegative_number(value, label="value")

    with pytest.raises(ValueError, match="value must be within the finite float range"):
        ensure_number_at_least(value, 1, label="value")

    with pytest.raises(ValueError, match="value must be within the finite float range"):
        ensure_number_in_closed_range(
            value,
            minimum=0,
            maximum=1,
            label="value",
        )


@pytest.mark.parametrize("value", [0.5, 1])
def test_ensure_positive_number_accepts_values_above_zero(
    value: int | float,
):
    result = ensure_positive_number(value, label="value")

    assert result == value
    assert isinstance(result, float)


@pytest.mark.parametrize("value", [-1, 0])
def test_ensure_positive_number_rejects_values_at_or_below_zero(
    value: int,
):
    with pytest.raises(ValueError, match="value must be a positive number"):
        ensure_positive_number(value, label="value")


@pytest.mark.parametrize("value", [0, 1])
def test_ensure_nonnegative_number_accepts_values_at_or_above_zero(
    value: int,
):
    result = ensure_nonnegative_number(value, label="value")

    assert result == value
    assert isinstance(result, float)


@pytest.mark.parametrize("value", [-1, -0.5])
def test_ensure_nonnegative_number_rejects_values_below_zero(
    value: int | float,
):
    with pytest.raises(ValueError, match="value must be a non-negative number"):
        ensure_nonnegative_number(value, label="value")


@pytest.mark.parametrize(
    ("value", "threshold"),
    [
        (1, 1),
        (2, 1),
        (0, -1),
        (1.5, 1.5),
    ],
)
def test_ensure_number_at_least_accepts_values_at_or_above_threshold(
    value: int | float,
    threshold: int | float,
):
    result = ensure_number_at_least(value, threshold, label="value")

    assert result == value
    assert isinstance(result, float)


@pytest.mark.parametrize(
    ("value", "threshold"),
    [
        (0, 1),
        (0.5, 1),
        (-2, -1),
        (1.4, 1.5),
    ],
)
def test_ensure_number_at_least_rejects_values_below_threshold(
    value: int | float,
    threshold: int | float,
):
    with pytest.raises(
        ValueError,
        match=f"value must be a number greater than or equal to {threshold}",
    ):
        ensure_number_at_least(value, threshold, label="value")


@pytest.mark.parametrize("threshold", [True, "1", math.nan, math.inf, -math.inf])
def test_ensure_number_at_least_requires_finite_threshold(
    threshold: object,
):
    with pytest.raises(ValueError, match="threshold must be a finite number"):
        ensure_number_at_least(1, threshold, label="value")  # type: ignore[arg-type]


def test_ensure_number_at_least_rejects_threshold_outside_float_range():
    with pytest.raises(
        ValueError,
        match="threshold must be within the finite float range",
    ):
        ensure_number_at_least(1, 10**1000, label="value")


@pytest.mark.parametrize(
    ("value", "minimum", "maximum"),
    [
        (0, 0, 1),
        (2, 1, 2),
        (0, -1, 1),
        (0.5, 0.25, 0.75),
        (1, 1, 1),
    ],
)
def test_ensure_number_in_closed_range_accepts_values_within_bounds(
    value: int | float,
    minimum: int | float,
    maximum: int | float,
):
    result = ensure_number_in_closed_range(
        value,
        minimum=minimum,
        maximum=maximum,
        label="value",
    )

    assert result == value
    assert isinstance(result, float)


@pytest.mark.parametrize(
    ("value", "minimum", "maximum"),
    [
        (-1, 0, 1),
        (3, 1, 2),
        (0.1, 0.25, 0.75),
    ],
)
def test_ensure_number_in_closed_range_rejects_values_outside_bounds(
    value: int | float,
    minimum: int | float,
    maximum: int | float,
):
    with pytest.raises(
        ValueError,
        match=f"value must be a number between {minimum} and {maximum}, inclusive",
    ):
        ensure_number_in_closed_range(
            value,
            minimum=minimum,
            maximum=maximum,
            label="value",
        )


@pytest.mark.parametrize("bound", [True, "1", math.nan, math.inf, -math.inf])
def test_ensure_number_in_closed_range_requires_finite_bounds(
    bound: object,
):
    with pytest.raises(ValueError, match="minimum must be a finite number"):
        ensure_number_in_closed_range(
            0,
            minimum=bound,  # type: ignore[arg-type]
            maximum=1,
            label="value",
        )

    with pytest.raises(ValueError, match="maximum must be a finite number"):
        ensure_number_in_closed_range(
            0,
            minimum=0,
            maximum=bound,  # type: ignore[arg-type]
            label="value",
        )


def test_ensure_number_in_closed_range_rejects_bounds_outside_float_range():
    with pytest.raises(
        ValueError,
        match="minimum must be within the finite float range",
    ):
        ensure_number_in_closed_range(
            0,
            minimum=-(10**1000),
            maximum=1,
            label="value",
        )

    with pytest.raises(
        ValueError,
        match="maximum must be within the finite float range",
    ):
        ensure_number_in_closed_range(
            0,
            minimum=0,
            maximum=10**1000,
            label="value",
        )


def test_ensure_number_in_closed_range_rejects_minimum_above_maximum():
    with pytest.raises(
        ValueError,
        match="minimum must be less than or equal to maximum",
    ):
        ensure_number_in_closed_range(
            1,
            minimum=2,
            maximum=1,
            label="value",
        )
