"""Generic data-quality rules.

Each rule returns a :class:`Measurement`; a :class:`Check` binds a rule to a table, period and severity and
evaluates it lazily, so a rule that raises (e.g. a missing table) is recorded as a failure instead of
aborting the whole suite.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

ERROR, WARN = "error", "warn"
ALL_PERIODS = "ALL"
_SAMPLE = 5  # offending values listed in ``details``


@dataclass(frozen=True)
class Measurement:
    passed: bool
    observed: str
    expected: str
    failed_rows: int | None = None
    details: str | None = None


@dataclass(frozen=True)
class CheckResult:
    check: str
    table: str
    rule: str
    severity: str
    period: str
    passed: bool
    observed: str | None
    expected: str | None
    failed_rows: int | None
    details: str | None

    @property
    def outcome(self) -> str:
        if self.passed:
            return "passed"
        return "failed" if self.severity == ERROR else "warning"


@dataclass(frozen=True)
class Check:
    name: str
    table: str
    rule: str
    measure: Callable[[], Measurement]
    severity: str = ERROR
    period: str = ALL_PERIODS

    def run(self) -> CheckResult:
        try:
            m = self.measure()
        except Exception as exc:  # noqa: BLE001 - a broken check is a failed check, not a crashed suite
            m = Measurement(False, "error", "check evaluates", details=f"{type(exc).__name__}: {exc}"[:2000])
        return CheckResult(
            check=self.name,
            table=self.table,
            rule=self.rule,
            severity=self.severity,
            period=self.period,
            passed=m.passed,
            observed=m.observed,
            expected=m.expected,
            failed_rows=m.failed_rows,
            details=m.details,
        )


def run_checks(checks: Iterable[Check]) -> list[CheckResult]:
    return [c.run() for c in checks]


def blocking_failures(results: Iterable[CheckResult]) -> list[CheckResult]:
    return [r for r in results if r.outcome == "failed"]


def _offending(bad: DataFrame, column: str, what: str, expected: str, observed: str | None = None) -> Measurement:
    """Pass when ``bad`` is empty; otherwise report its row count and a few offending values."""
    failed = bad.count()
    sample = [r[0] for r in bad.select(column).distinct().limit(_SAMPLE).collect()] if failed else []
    return Measurement(
        passed=failed == 0,
        observed=observed or f"{failed} {what}",
        expected=expected,
        failed_rows=failed,
        details=f"e.g. {sample}" if failed else None,
    )


def not_null(df: DataFrame, *columns: str) -> Measurement:
    counts = df.agg(*[F.count_if(F.col(c).isNull()).alias(c) for c in columns]).first().asDict()
    failed = df.filter(" OR ".join(f"`{c}` IS NULL" for c in columns)).count() if any(counts.values()) else 0
    nulls = {c: n for c, n in counts.items() if n}
    return Measurement(
        failed == 0, f"{failed} rows with nulls", "0 rows with nulls", failed, str(nulls) if nulls else None
    )


def unique(df: DataFrame, *columns: str) -> Measurement:
    """Fails with the number of rows whose key appears more than once."""
    dupes = df.groupBy(*columns).count().filter("count > 1")
    failed = dupes.agg(F.coalesce(F.sum("count"), F.lit(0))).first()[0]
    sample = [r[0] for r in dupes.select(columns[0]).limit(_SAMPLE).collect()] if failed else []
    details = f"e.g. {sample}" if failed else None
    return Measurement(failed == 0, f"{failed} rows sharing a key", "0 rows sharing a key", failed, details)


def accepted_values(df: DataFrame, column: str, values: Iterable[str]) -> Measurement:
    values = list(values)
    bad = df.filter(~F.col(column).isin(values))  # nulls are not_null's job
    return _offending(bad, column, "rows outside set", f"{column} in {values}")


def in_range(df: DataFrame, column: str, low: float, high: float) -> Measurement:
    lo, hi = df.agg(F.min(column), F.max(column)).first()
    bad = df.filter((F.col(column) < low) | (F.col(column) > high))
    return _offending(bad, column, "rows out of range", f"{low} <= {column} <= {high}", f"min={lo} max={hi}")


def foreign_key(child: DataFrame, column: str, parent: DataFrame, parent_column: str) -> Measurement:
    keys = parent.select(F.col(parent_column).alias(column)).distinct()
    orphans = child.filter(F.col(column).isNotNull()).join(keys, column, "left_anti")
    return _offending(orphans, column, "orphan rows", "0 orphan rows")


def row_count(actual: DataFrame, expected: int) -> Measurement:
    n = actual.count()
    return Measurement(n == expected, str(n), str(expected))


def reconcile(actual: int, expected: int, label: str = "") -> Measurement:
    """Two counts that must match exactly (e.g. Silver rows in == Gold rows out)."""
    diff = actual - expected
    return Measurement(diff == 0, str(actual), str(expected), abs(diff), f"{label} diff={diff:+d}" if diff else None)


def max_ratio(numerator: int, denominator: int, threshold: float) -> Measurement:
    ratio = numerator / denominator if denominator else 0.0
    return Measurement(ratio <= threshold, f"{ratio:.4f}", f"<= {threshold}", details=f"{numerator}/{denominator}")


def min_period(latest: str | None, minimum: str) -> Measurement:
    """The most recent loaded ``YYYYMM`` period is at least ``minimum`` (source keeps being loaded)."""
    return Measurement(latest is not None and latest >= minimum, str(latest), f">= {minimum}")


def freshness(last: datetime | None, max_age: timedelta, now: datetime) -> Measurement:
    if last is None:
        return Measurement(False, "never", f"<= {max_age}")
    age = now - last
    return Measurement(age <= max_age, str(age).split(".")[0], f"<= {max_age}", details=f"last={last.isoformat()}")
