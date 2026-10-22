"""Pre-aggregated Gold tables, rebuilt per period from that period's ``fact_trips`` partition.

* ``agg_daily_kpis``      one row per day x member type, with the day's weather
* ``agg_station_monthly`` one row per period x station: starts, ends and net flow (rebalancing signal)
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F

from divvy_medallion.bronze import PERIOD_COLUMN

DAILY_KPIS_TABLE = "agg_daily_kpis"
STATION_MONTHLY_TABLE = "agg_station_monthly"


def _share(flag: str) -> Column:
    return F.round(F.avg(F.col(flag).cast("double")), 4)


def build_daily_kpis(fact: DataFrame, dim_weather: DataFrame) -> DataFrame:
    kpis = fact.groupBy("date_key", "member_type", PERIOD_COLUMN).agg(
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
        PERIOD_COLUMN,
    )


def build_station_monthly(fact: DataFrame) -> DataFrame:
    starts = fact.groupBy(PERIOD_COLUMN, F.col("start_station_key").alias("station_key")).agg(
        F.count("*").alias("trips_started"),
        F.sum((F.col("member_type") == "member").cast("int")).alias("member_trips_started"),
        F.round(F.avg("duration_min"), 2).alias("avg_duration_min"),
    )
    ends = fact.groupBy(PERIOD_COLUMN, F.col("end_station_key").alias("station_key")).agg(
        F.count("*").alias("trips_ended")
    )
    return (
        starts.join(ends, [PERIOD_COLUMN, "station_key"], "full")
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
            PERIOD_COLUMN,
        )
    )
