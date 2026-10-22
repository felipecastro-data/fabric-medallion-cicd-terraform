from datetime import datetime, timedelta

import pytest

from divvy_medallion.dq import rules
from divvy_medallion.dq.rules import ERROR, WARN, Check, Measurement


@pytest.fixture(scope="module")
def df(spark):
    rows = [("a", "member", 5.0), ("b", "casual", 12.0), ("b", None, 99.0), (None, "ghost", 1.0)]
    return spark.createDataFrame(rows, "id string, member_type string, minutes double")


def test_not_null_counts_rows_and_reports_columns(df):
    m = rules.not_null(df, "id", "member_type")
    assert (m.passed, m.failed_rows) == (False, 2)
    assert m.details == "{'id': 1, 'member_type': 1}"
    assert rules.not_null(df, "minutes").passed


def test_unique_counts_every_row_sharing_a_key(df):
    m = rules.unique(df, "id")
    assert (m.passed, m.failed_rows, m.details) == (False, 2, "e.g. ['b']")
    assert rules.unique(df, "minutes").passed


def test_accepted_values_ignores_nulls(df):
    m = rules.accepted_values(df, "member_type", ["member", "casual"])
    assert (m.passed, m.failed_rows, m.details) == (False, 1, "e.g. ['ghost']")


def test_in_range_reports_min_and_max(df):
    m = rules.in_range(df, "minutes", 1, 60)
    assert (m.passed, m.failed_rows, m.observed) == (False, 1, "min=1.0 max=99.0")


def test_foreign_key_finds_orphans(spark, df):
    parent = spark.createDataFrame([("a",)], "id string")
    m = rules.foreign_key(df, "id", parent, "id")
    assert (m.passed, m.failed_rows, m.details) == (False, 2, "e.g. ['b']")  # nulls are not orphans


def test_scalar_rules():
    assert rules.reconcile(10, 10).passed
    off = rules.reconcile(9, 10, "fact vs silver")
    assert (off.passed, off.failed_rows, off.details) == (False, 1, "fact vs silver diff=-1")
    assert rules.max_ratio(5, 100, 0.05).passed and not rules.max_ratio(6, 100, 0.05).passed
    assert rules.max_ratio(0, 0, 0.05).passed
    assert rules.min_period("202609", "202608").passed and not rules.min_period(None, "202608").passed


def test_freshness():
    now = datetime(2026, 10, 22, 12)
    assert rules.freshness(now - timedelta(hours=3), timedelta(hours=48), now).passed
    assert not rules.freshness(now - timedelta(days=3), timedelta(hours=48), now).passed
    assert rules.freshness(None, timedelta(hours=48), now).observed == "never"


def test_check_outcome_depends_on_severity_and_errors_become_failures():
    failing = Measurement(False, "1", "0")
    assert Check("c", "t", "r", lambda: failing, ERROR).run().outcome == "failed"
    assert Check("c", "t", "r", lambda: failing, WARN).run().outcome == "warning"

    def broken():
        raise FileNotFoundError("table not found")

    result = Check("c", "t", "r", broken, WARN, "202609").run()
    assert (result.passed, result.period, result.details) == (False, "202609", "FileNotFoundError: table not found")
    assert rules.blocking_failures([result, Check("c", "t", "r", lambda: failing).run()])[0].severity == ERROR
