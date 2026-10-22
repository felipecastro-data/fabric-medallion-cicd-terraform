"""Silver ``trips`` -> Gold ``fact_trips`` (one row per valid trip, partitioned by ``_period``)."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from divvy_medallion.bronze import PERIOD_COLUMN
from divvy_medallion.gold import date_key, station_key

FACT_TRIPS_TABLE = "fact_trips"


def build_fact_trips(trips: DataFrame) -> DataFrame:
    return trips.select(
        "ride_id",
        date_key(F.col("trip_date")).alias("date_key"),
        "start_hour",
        station_key(F.col("start_station_id")).alias("start_station_key"),
        station_key(F.col("end_station_id")).alias("end_station_key"),
        "rideable_type",
        "member_type",
        "started_at",
        "ended_at",
        F.round(F.col("duration_s") / 60, 2).alias("duration_min"),
        "distance_km",
        "is_round_trip",
        "is_dockless",
        (F.col("rideable_type") != "classic_bike").alias("is_electric"),
        F.col("_batch_id").alias("_bronze_batch_id"),  # lineage back to the Bronze load
        PERIOD_COLUMN,
    )
