"""Pre-aggregated Gold tables, partitioned by the **trip's own month** (``trip_month``).

* ``agg_daily_kpis``      one row per day x member type, with the day's weather
* ``agg_station_monthly`` one row per trip month x station: starts, ends and net flow (rebalancing signal)

Divvy's monthly files overlap at the boundary: the ``202609`` file also holds a few trips that started on
2026-08-31. Aggregating per *source file* would split that day across two rows, so a trip month ``M`` is
always recomputed from every file that can contain it (``M`` and ``M+1``) whenever either one is reloaded.
"""

from __future__ import annotations

from datetime import datetime

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from divvy_medallion.bronze import PERIOD_COLUMN
from divvy_medallion.periods import add_months

DAILY_KPIS_TABLE = "agg_daily_kpis"
STATION_MONTHLY_TABLE = "agg_station_monthly"
TRIP_MONTH_COLUMN = "trip_month"


def trip_month(date_key: Column) -> Column:
    """``yyyyMM`` of a ``yyyyMMdd`` date key."""
    return F.substring(date_key.cast("string"), 1, 6)


def source_periods(month: str) -> list[str]:
    """Source-file periods that can contain trips started in ``month`` (late trips spill into the next file)."""
    return [month, add_months(month, 1)]


def month_facts(fact: DataFrame, month: str) -> DataFrame:
    """Every fact row whose trip started in ``month``, across all source files (partition-pruned)."""
    return fact.filter(F.col(PERIOD_COLUMN).isin(source_periods(month)) & (trip_month(F.col("date_key")) == month))


def months_to_refresh(fact_loaded_at: dict[str, datetime], aggregated_at: dict[str, datetime]) -> list[str]:
    """Trip months whose aggregates are older than a fact load of any source file that feeds them.

    ``fact_loaded_at``: source period -> last successful fact load; ``aggregated_at``: trip month -> last
    successful aggregate build. Tracked per month, so a failed aggregate build is repaired by a plain retry.
    """
    candidates = {m for p in fact_loaded_at for m in (p, add_months(p, -1))}
    stale = []
    for month in sorted(candidates):
        fed_at = max(fact_loaded_at[p] for p in source_periods(month) if p in fact_loaded_at)
        if month not in aggregated_at or aggregated_at[month] < fed_at:
            stale.append(month)
    return stale


def _share(flag: str) -> Column:
    return F.round(F.avg(F.col(flag).cast("double")), 4)


def build_daily_kpis(fact: DataFrame, dim_weather: DataFrame) -> DataFrame:
    kpis = fact.groupBy("date_key", "member_type").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("duration_min"), 2).alias("total_duration_min"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
        F.percentile_approx("duration_min", 0.5).alias("median_duration_min"),
        F.round(F.avg("distance_km"), 3).alias("avg_distance_km"),
        _share("is_electric").alias("electric_share"),
        _share("is_round_trip").alias("round_trip_share"),
        _share("is_dockless").alias("dockless_share"),
    )
    weather = dim_weather.select("date_key", "condition", "temperature_2m_mean", "precipitation_sum", "is_wet_day")
    return kpis.join(F.broadcast(weather), "date_key", "left").select(
        "date_key",
        "member_type",
        "trips",
        "total_duration_min",
        "avg_duration_min",
        "median_duration_min",
        "avg_distance_km",
        "electric_share",
        "round_trip_share",
        "dockless_share",
        "condition",
        "temperature_2m_mean",
        "precipitation_sum",
        "is_wet_day",
        trip_month(F.col("date_key")).alias(TRIP_MONTH_COLUMN),
    )


def build_station_monthly(fact: DataFrame) -> DataFrame:
    fact = fact.withColumn(TRIP_MONTH_COLUMN, trip_month(F.col("date_key")))
    starts = fact.groupBy(TRIP_MONTH_COLUMN, F.col("start_station_key").alias("station_key")).agg(
        F.count("*").alias("trips_started"),
        F.sum((F.col("member_type") == "member").cast("int")).alias("member_trips_started"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
    )
    ends = fact.groupBy(TRIP_MONTH_COLUMN, F.col("end_station_key").alias("station_key")).agg(
        F.count("*").alias("trips_ended")
    )
    return (
        starts.join(ends, [TRIP_MONTH_COLUMN, "station_key"], "full")
        .fillna(0, subset=["trips_started", "member_trips_started", "trips_ended"])
        .withColumn("net_flow", F.col("trips_ended") - F.col("trips_started"))  # > 0: bikes accumulate
        .withColumn(
            "member_share",
            F.when(F.col("trips_started") > 0, F.round(F.col("member_trips_started") / F.col("trips_started"), 4)),
        )
        .select(
            "station_key",
            "trips_started",
            "trips_ended",
            "net_flow",
            "member_trips_started",
            "member_share",
            "avg_duration_min",
            TRIP_MONTH_COLUMN,
        )
    )
