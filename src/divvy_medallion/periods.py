"""Month-period helpers. Periods are ``YYYYMM`` strings, matching Divvy file names and Bronze partitions."""

from __future__ import annotations

import re
from datetime import date

_PERIOD = re.compile(r"^\d{4}(0[1-9]|1[0-2])$")


def validate_period(period: str) -> str:
    if not _PERIOD.match(period):
        raise ValueError(f"Invalid period '{period}', expected YYYYMM")
    return period


def add_months(period: str, months: int) -> str:
    year, month = int(period[:4]), int(period[4:])
    index = year * 12 + (month - 1) + months
    return f"{index // 12:04d}{index % 12 + 1:02d}"


def month_range(start: str, end: str) -> list[str]:
    validate_period(start)
    validate_period(end)
    if start > end:
        raise ValueError(f"Start period {start} is after end period {end}")
    periods = [start]
    while periods[-1] < end:
        periods.append(add_months(periods[-1], 1))
    return periods


def parse_periods(spec: str, today: date | None = None) -> list[str]:
    """Parse a period spec into an ordered list of ``YYYYMM`` periods.

    Supported forms:
      * ``202609``           a single month
      * ``202510-202609``    an inclusive range
      * ``last:12``          the last N complete months before ``today``
      * ``202601,202603``    a comma-separated list of any of the above
    """
    today = today or date.today()
    periods: set[str] = set()
    for part in (p.strip() for p in spec.split(",") if p.strip()):
        if part.startswith("last:"):
            count = int(part.split(":", 1)[1])
            if count < 1:
                raise ValueError("last:N requires N >= 1")
            end = add_months(f"{today.year:04d}{today.month:02d}", -1)
            periods.update(month_range(add_months(end, -(count - 1)), end))
        elif "-" in part:
            start, end = (s.strip() for s in part.split("-", 1))
            periods.update(month_range(start, end))
        else:
            periods.add(validate_period(part))
    if not periods:
        raise ValueError("Period spec is empty")
    return sorted(periods)


def period_bounds(period: str) -> tuple[date, date]:
    """First and last calendar day of a period."""
    validate_period(period)
    first = date(int(period[:4]), int(period[4:]), 1)
    nxt = add_months(period, 1)
    last = date.fromordinal(date(int(nxt[:4]), int(nxt[4:]), 1).toordinal() - 1)
    return first, last
