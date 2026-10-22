"""The project's data-quality suite over Bronze -> Silver -> Gold.

Per-period checks run for each requested period loaded in ``fact_trips``; table-wide checks run once
(period ``ALL``). ``error`` checks guard correctness (keys, reconciliation, referential integrity) and block
the pipeline; ``warn`` checks flag drift worth a look (outliers, quarantine rate, freshness).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import BooleanType, LongType, StringType, StructField, StructType, TimestampType

from divvy_medallion.bronze import PERIOD_COLUMN
from divvy_medallion.dq import rules
from divvy_medallion.dq.rules import ERROR, WARN, Check, CheckResult
from divvy_medallion.gold import DOCKLESS_STATION_KEY
from divvy_medallion.periods import add_months
from divvy_medallion.silver.trips import MAX_DURATION_S, MEMBER_TYPES, MIN_DURATION_S, RIDEABLE_TYPES

DQ_RESULTS_TABLE = "dq_results"

QUARANTINE_WARN_RATE = 0.05  # ~2.5% is normal for Divvy (mostly sub-60 s false starts)
QUARANTINE_MAX_RATE = 0.15  # beyond this the source file is likely broken
MAX_DISTANCE_KM = 50.0  # straight-line; the service area is ~55 km tall, so longer is a GPS glitch
MAX_BUILD_AGE = timedelta(hours=48)
MAX_SOURCE_LAG_MONTHS = 2  # Divvy publishes each month a few weeks after it ends


@dataclass(frozen=True)
class Tables:
    bronze_trips: DataFrame
    silver_trips: DataFrame
    quarantine: DataFrame
    fact_trips: DataFrame
    dim_date: DataFrame
    dim_station: DataFrame
    dim_weather: DataFrame
    daily_kpis: DataFrame
    station_monthly: DataFrame


def _p(df: DataFrame, period: str) -> DataFrame:
    return df.filter(F.col(PERIOD_COLUMN) == period)


def _sum(df: DataFrame, column: str) -> int:
    return df.agg(F.coalesce(F.sum(column), F.lit(0))).first()[0]


def table_checks(t: Tables, *, latest_bronze_period: str | None, gold_built_at: datetime | None,
                 today: date, now: datetime) -> list[Check]:  # fmt: skip
    min_period = add_months(f"{today:%Y%m}", -MAX_SOURCE_LAG_MONTHS)
    return [
        Check("dim_date.unique_date_key", "dim_date", "unique", lambda: rules.unique(t.dim_date, "date_key")),
        Check("dim_station.unique_station_key", "dim_station", "unique",
              lambda: rules.unique(t.dim_station, "station_key")),
        Check("dim_station.not_null", "dim_station", "not_null",
              lambda: rules.not_null(t.dim_station, "station_key", "station_id", "station_name")),
        Check("dim_station.dockless_member", "dim_station", "row_count",
              lambda: rules.row_count(t.dim_station.filter(F.col("station_key") == DOCKLESS_STATION_KEY), 1)),
        Check("dim_weather.unique_date_key", "dim_weather", "unique", lambda: rules.unique(t.dim_weather, "date_key")),
        Check("dim_weather.temperature_plausible", "dim_weather", "in_range",
              lambda: rules.in_range(t.dim_weather, "temperature_2m_mean", -35.0, 45.0), WARN),
        # a ride re-published in a later month's file would be double counted across periods
        Check("fact_trips.unique_ride_id_all_periods", "fact_trips", "unique",
              lambda: rules.unique(t.fact_trips, "ride_id"), WARN),
        Check("bronze.divvy_trips.freshness", "divvy_trips", "min_period",
              lambda: rules.min_period(latest_bronze_period, min_period), WARN),
        Check("gold.build_freshness", "processing_log", "freshness",
              lambda: rules.freshness(gold_built_at, MAX_BUILD_AGE, now), WARN),
    ]  # fmt: skip


def period_checks(t: Tables, period: str) -> list[Check]:
    fact, silver, bronze = _p(t.fact_trips, period), _p(t.silver_trips, period), _p(t.bronze_trips, period)
    quarantine = _p(t.quarantine, period)

    def check(name: str, table: str, rule: str, measure, severity: str = ERROR) -> Check:
        return Check(name, table, rule, measure, severity, period)

    return [
        check("fact_trips.not_null_keys", "fact_trips", "not_null", lambda: rules.not_null(
            fact, "ride_id", "date_key", "start_station_key", "end_station_key", "member_type", "rideable_type")),
        check("fact_trips.unique_ride_id", "fact_trips", "unique", lambda: rules.unique(fact, "ride_id")),
        check("fact_trips.member_type", "fact_trips", "accepted_values",
              lambda: rules.accepted_values(fact, "member_type", MEMBER_TYPES)),
        check("fact_trips.rideable_type", "fact_trips", "accepted_values",
              lambda: rules.accepted_values(fact, "rideable_type", RIDEABLE_TYPES)),
        check("fact_trips.duration_min", "fact_trips", "in_range",
              lambda: rules.in_range(fact, "duration_min", MIN_DURATION_S / 60, MAX_DURATION_S / 60)),
        check("fact_trips.distance_km", "fact_trips", "in_range",
              lambda: rules.in_range(fact, "distance_km", 0.0, MAX_DISTANCE_KM), WARN),
        check("fact_trips.fk_date", "fact_trips", "foreign_key",
              lambda: rules.foreign_key(fact, "date_key", t.dim_date, "date_key")),
        check("fact_trips.fk_start_station", "fact_trips", "foreign_key",
              lambda: rules.foreign_key(fact, "start_station_key", t.dim_station, "station_key")),
        check("fact_trips.fk_end_station", "fact_trips", "foreign_key",
              lambda: rules.foreign_key(fact, "end_station_key", t.dim_station, "station_key")),
        check("fact_trips.weather_coverage", "fact_trips", "foreign_key",
              lambda: rules.foreign_key(fact, "date_key", t.dim_weather, "date_key"), WARN),
        check("reconcile.bronze_to_silver", "trips", "reconcile",
              lambda: rules.reconcile(silver.count() + quarantine.count(), bronze.count(), "valid+quarantine")),
        check("reconcile.silver_to_fact", "fact_trips", "reconcile",
              lambda: rules.reconcile(fact.count(), silver.count(), "fact vs silver")),
        check("reconcile.fact_to_daily_kpis", "agg_daily_kpis", "reconcile",
              lambda: rules.reconcile(_sum(_p(t.daily_kpis, period), "trips"), fact.count(), "kpis vs fact")),
        check("reconcile.fact_to_station_monthly", "agg_station_monthly", "reconcile",
              lambda: rules.reconcile(_sum(_p(t.station_monthly, period), "trips_started"), fact.count(),
                                      "station starts vs fact")),
        check("silver.quarantine_rate_warn", "trips_quarantine", "max_ratio",
              lambda: rules.max_ratio(quarantine.count(), bronze.count(), QUARANTINE_WARN_RATE), WARN),
        check("silver.quarantine_rate_max", "trips_quarantine", "max_ratio",
              lambda: rules.max_ratio(quarantine.count(), bronze.count(), QUARANTINE_MAX_RATE)),
    ]  # fmt: skip


DQ_RESULTS_SCHEMA = StructType(
    [
        StructField("run_id", StringType(), False),
        StructField("checked_at", TimestampType(), False),
        StructField("check_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("rule", StringType(), False),
        StructField("severity", StringType(), False),
        StructField("period", StringType(), False),
        StructField("outcome", StringType(), False),
        StructField("passed", BooleanType(), False),
        StructField("failed_rows", LongType(), True),
        StructField("observed", StringType(), True),
        StructField("expected", StringType(), True),
        StructField("details", StringType(), True),
    ]
)


def results_frame(spark: SparkSession, run_id: str, checked_at: datetime, results: list[CheckResult]) -> DataFrame:
    """One row per check result, appended to ``dq_results`` (history feeds DQ trends)."""
    rows = [
        (run_id, checked_at, r.check, r.table, r.rule, r.severity, r.period, r.outcome, r.passed, r.failed_rows,
         r.observed, r.expected, r.details)
        for r in results
    ]  # fmt: skip
    return spark.createDataFrame(rows, schema=DQ_RESULTS_SCHEMA)
