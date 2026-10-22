"""Station dimension derived from Silver trips (full rebuild; a few thousand rows).

One row per ``station_id``: its most frequent name, the median of observed coordinates (robust to GPS
noise on e-bikes) and first/last day the station appeared in a trip.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

STATIONS_TABLE = "stations"


def _endpoints(trips: DataFrame) -> DataFrame:
    def side(prefix: str) -> DataFrame:
        return trips.select(
            F.col(f"{prefix}_station_id").alias("station_id"),
            F.col(f"{prefix}_station_name").alias("station_name"),
            F.col(f"{prefix}_lat").alias("lat"),
            F.col(f"{prefix}_lng").alias("lng"),
            F.col("trip_date"),
        )

    return side("start").unionByName(side("end")).filter(F.col("station_id").isNotNull())


def build_stations(trips: DataFrame) -> DataFrame:
    points = _endpoints(trips)

    by_name = Window.partitionBy("station_id").orderBy(F.col("uses").desc(), F.col("station_name"))
    names = (
        points.filter(F.col("station_name").isNotNull())
        .groupBy("station_id", "station_name")
        .agg(F.count("*").alias("uses"))
        .withColumn("_rn", F.row_number().over(by_name))
        .filter("_rn = 1")
        .select("station_id", "station_name")
    )
    stats = points.groupBy("station_id").agg(
        F.round(F.percentile_approx("lat", 0.5), 6).alias("latitude"),
        F.round(F.percentile_approx("lng", 0.5), 6).alias("longitude"),
        F.min("trip_date").alias("first_seen"),
        F.max("trip_date").alias("last_seen"),
        F.count("*").alias("trip_endpoints"),
        F.countDistinct("station_name").alias("name_variants"),
    )
    return stats.join(names, "station_id", "left").select(
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "first_seen",
        "last_seen",
        "trip_endpoints",
        "name_variants",
    )
