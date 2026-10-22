from datetime import date

import pytest

from divvy_medallion.periods import add_months, month_range, parse_periods, period_bounds


def test_add_months_crosses_year_boundaries():
    assert add_months("202512", 1) == "202601"
    assert add_months("202601", -1) == "202512"
    assert add_months("202609", -11) == "202510"


def test_month_range_is_inclusive():
    assert month_range("202511", "202602") == ["202511", "202512", "202601", "202602"]


@pytest.mark.parametrize(
    ("spec", "expected"),
    [
        ("202609", ["202609"]),
        ("202607-202609", ["202607", "202608", "202609"]),
        ("202609, 202607", ["202607", "202609"]),
        ("last:3", ["202607", "202608", "202609"]),
    ],
)
def test_parse_periods(spec, expected):
    assert parse_periods(spec, today=date(2026, 10, 21)) == expected


def test_last_n_returns_complete_months_only():
    periods = parse_periods("last:12", today=date(2026, 10, 21))
    assert periods[0] == "202510" and periods[-1] == "202609" and len(periods) == 12


@pytest.mark.parametrize("spec", ["", "2026-09", "202613", "202609-202601", "last:0"])
def test_invalid_specs_raise(spec):
    with pytest.raises(ValueError):
        parse_periods(spec, today=date(2026, 10, 21))


def test_period_bounds_handles_month_lengths():
    assert period_bounds("202602") == (date(2026, 2, 1), date(2026, 2, 28))
    assert period_bounds("202512") == (date(2025, 12, 1), date(2025, 12, 31))
