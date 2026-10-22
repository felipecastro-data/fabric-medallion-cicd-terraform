from dataclasses import replace
from datetime import date, datetime

from divvy_medallion.dq.rules import blocking_failures, run_checks
from divvy_medallion.dq.suite import DQ_RESULTS_SCHEMA, month_checks, period_checks, results_frame, table_checks

NOW = datetime(2026, 10, 22, 12)


def _run(tables, gold_built_at=NOW):
    checks = table_checks(tables, latest_bronze_period="202609", gold_built_at=gold_built_at, today=NOW.date(), now=NOW)
    return run_checks([*checks, *period_checks(tables, "202609"), *month_checks(tables, "202609")])


def test_consistent_medallion_passes_every_check(medallion):
    results = _run(medallion)

    not_passed = [(r.check, r.observed, r.details) for r in results if not r.passed]
    assert not not_passed
    assert {r.period for r in results} == {"ALL", "202609"}


def test_a_lost_fact_row_fails_reconciliation_and_blocks(medallion):
    broken = replace(medallion, fact_trips=medallion.fact_trips.filter("ride_id != 'R3'"))
    blocking = {r.check for r in blocking_failures(_run(broken))}

    assert {"reconcile.silver_to_fact", "reconcile.fact_to_daily_kpis"} <= blocking


def test_an_unknown_station_key_fails_referential_integrity(medallion):
    broken = replace(medallion, dim_station=medallion.dim_station.filter("station_id != 'S2'"))
    failed = {r.check: r for r in blocking_failures(_run(broken))}

    assert {"fact_trips.fk_start_station", "fact_trips.fk_end_station"} <= set(failed)


def test_stale_build_only_warns(medallion):
    results = {r.check: r for r in _run(medallion, gold_built_at=datetime(2026, 10, 1))}

    assert results["gold.build_freshness"].outcome == "warning"
    assert not blocking_failures(results.values())


def test_results_frame_matches_the_dq_results_schema(spark, medallion):
    results = _run(medallion)
    df = results_frame(spark, "run-1", NOW, results)

    assert df.schema == DQ_RESULTS_SCHEMA
    assert df.count() == len(results)
    assert df.filter("outcome = 'passed'").count() == len(results)
    assert date(2026, 10, 22) == df.first().checked_at.date()


def test_a_day_split_across_source_files_fails_the_aggregate_grain_check(medallion):
    split = medallion.daily_kpis.filter("date_key = 20260901")
    broken = replace(medallion, daily_kpis=medallion.daily_kpis.unionByName(split))
    blocking = {r.check for r in blocking_failures(_run(broken))}

    assert "agg_daily_kpis.unique_grain" in blocking
